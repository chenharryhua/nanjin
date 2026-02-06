package mtest

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.circe.{jawn, Json}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, DecoderFactory, EncoderFactory}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import scala.util.{Try, Using}

package object terminals {
  def genericRecord2Jackson(genericRecord: GenericRecord): Try[String] =
    Using(new ByteArrayOutputStream()) { baos =>
      val encoder = EncoderFactory.get().jsonEncoder(genericRecord.getSchema, baos)
      new GenericDatumWriter[GenericRecord](genericRecord.getSchema).write(genericRecord, encoder)
      encoder.flush()
      baos.toString(StandardCharsets.UTF_8)
    }(_.close())

  def genericRecord2Circe(genericRecord: GenericRecord): Try[Json] =
    genericRecord2Jackson(genericRecord).flatMap(jawn.parse(_).toTry)

  def genericRecord2BinAvro(genericRecord: GenericRecord): Try[Array[Byte]] =
    Using(new ByteArrayOutputStream) { baos =>
      val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(baos, null)
      new GenericDatumWriter[GenericRecord](genericRecord.getSchema).write(genericRecord, encoder)
      encoder.flush()
      baos.toByteArray
    }(_.close())

  def jackson2GenericRecord(schema: Schema, jackson: String): Try[GenericData.Record] =
    Using(new ByteArrayInputStream(jackson.getBytes)) { bais =>
      val jsonDecoder = DecoderFactory.get().jsonDecoder(schema, bais)
      val datumReader = new GenericDatumReader[GenericData.Record](schema)
      datumReader.read(null, jsonDecoder)
    }(_.close())

  val objectMapper = new ObjectMapper
  def genericRecord2JsonNode(record: GenericRecord): Try[JsonNode] =
    Using(new ByteArrayOutputStream) { baos =>
      val encoder = EncoderFactory.get().jsonEncoder(record.getSchema, baos)
      val writer = new GenericDatumWriter[GenericRecord](record.getSchema)
      writer.write(record, encoder)
      encoder.flush()
      baos.close()

      objectMapper.readTree(baos.toByteArray)
    }

  def jsonNode2GenericRecord(json: JsonNode, schema: Schema): Try[GenericRecord] =
    Using(new ByteArrayInputStream(json.toString.getBytes("UTF-8"))) { in =>
      val reader = new GenericDatumReader[GenericRecord](schema)
      val decoder = DecoderFactory.get().jsonDecoder(schema, in)
      reader.read(null, decoder)
    }

}
