package com.github.chenharryhua.nanjin.kafka.schema

import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.kafka.serdes.globalObjectMapper
import io.circe.{jawn, Json}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import scala.util.{Try, Using}

/** using the schema to reshape the input generic record
  *
  * @param schema
  *   target schema
  * @param getGenericRecord
  *   input generic record - could fail
  * @return
  *   generic record which has the given schema
  */
def immigrate(schema: Schema, getGenericRecord: => GenericRecord): Try[GenericData.Record] =
  Using(new ByteArrayOutputStream()) { baos =>
    val gr: GenericRecord = getGenericRecord
    if (gr eq null) null
    else {
      val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(baos, null)
      new GenericDatumWriter[GenericRecord](gr.getSchema).write(gr, encoder)
      encoder.flush()

      val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(baos.toByteArray, null)
      new GenericDatumReader[GenericData.Record](gr.getSchema, schema).read(null, decoder)
    }
  }

def genericRecord2Jackson(genericRecord: GenericRecord): Try[String] =
  Using(new ByteArrayOutputStream()) { baos =>
    val encoder = EncoderFactory.get().jsonEncoder(genericRecord.getSchema, baos)
    new GenericDatumWriter[GenericRecord](genericRecord.getSchema).write(genericRecord, encoder)
    encoder.flush()
    baos.toString(StandardCharsets.UTF_8)
  }

def genericRecord2Circe(genericRecord: GenericRecord): Try[Json] =
  genericRecord2Jackson(genericRecord).flatMap(jawn.parse(_).toTry)

def genericRecord2BinAvro(genericRecord: GenericRecord): Try[Array[Byte]] =
  Using(new ByteArrayOutputStream) { baos =>
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(baos, null)
    new GenericDatumWriter[GenericRecord](genericRecord.getSchema).write(genericRecord, encoder)
    encoder.flush()
    baos.toByteArray
  }

def jackson2GenericRecord(schema: Schema, jackson: String): Try[GenericData.Record] =
  Using(new ByteArrayInputStream(jackson.getBytes)) { bais =>
    val jsonDecoder = DecoderFactory.get().jsonDecoder(schema, bais)
    val datumReader = new GenericDatumReader[GenericData.Record](schema)
    datumReader.read(null, jsonDecoder)
  }

def genericRecord2JsonNode(record: GenericRecord): Try[JsonNode] =
  Using(new ByteArrayOutputStream) { baos =>
    val encoder = EncoderFactory.get().jsonEncoder(record.getSchema, baos)
    val writer = new GenericDatumWriter[GenericRecord](record.getSchema)
    writer.write(record, encoder)
    encoder.flush()
    baos.close()

    globalObjectMapper.readTree(baos.toByteArray)
  }

def jsonNode2GenericRecord(json: JsonNode, schema: Schema): Try[GenericRecord] =
  Using(new ByteArrayInputStream(json.toString.getBytes("UTF-8"))) { in =>
    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().jsonDecoder(schema, in)
    reader.read(null, decoder)
  }
