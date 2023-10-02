package com.github.chenharryhua.nanjin.messages.kafka

import com.github.chenharryhua.nanjin.common.optics.jsonPlated
import io.circe.{parser, Json}
import monocle.function.Plated
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.*
import org.apache.avro.{Schema, SchemaCompatibility}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.{Try, Using}

package object codec {
  def backwardCompatibility(a: Schema, b: Schema): List[SchemaCompatibility.Incompatibility] =
    SchemaCompatibility.checkReaderWriterCompatibility(a, b).getResult.getIncompatibilities.asScala.toList

  def forwardCompatibility(a: Schema, b: Schema): List[SchemaCompatibility.Incompatibility] =
    backwardCompatibility(b, a)

  /** remove all default fields in the schema
    * @param schema
    *   input schema
    * @return
    *   schema without default fields
    */
  def removeDefaultField(schema: Schema): Schema = {
    val remove: Json => Json = Plated.transform[Json] { js =>
      js.asObject match {
        case Some(value) => value.toJson.hcursor.downField("default").delete.top.getOrElse(js)
        case None        => js
      }
    }

    parser
      .parse(schema.toString(false))
      .toOption
      .map(remove)
      .map(js => (new Schema.Parser).parse(js.noSpaces))
      .getOrElse(schema)
  }

  /** remove namespace field from the schema
    *
    * @param schema
    *   input schema
    * @return
    *   schema without namespace
    */
  def removeNamespace(schema: Schema): Schema = {
    val remove: Json => Json = Plated.transform[Json] { js =>
      js.asObject match {
        case Some(value) => value.toJson.hcursor.downField("namespace").delete.top.getOrElse(js)
        case None        => js
      }
    }

    parser
      .parse(schema.toString(false))
      .toOption
      .map(remove)
      .map(js => (new Schema.Parser).parse(js.noSpaces))
      .getOrElse(schema)
  }

  /** remove doc field from the schema
    *
    * @param schema
    *   input schema
    * @return
    *   schema without doc
    */
  def removeDocField(schema: Schema): Schema = {
    val remove: Json => Json = Plated.transform[Json] { js =>
      js.asObject match {
        case Some(value) => value.toJson.hcursor.downField("doc").delete.top.getOrElse(js)
        case None        => js
      }
    }

    parser
      .parse(schema.toString(false))
      .toOption
      .map(remove)
      .map(js => (new Schema.Parser).parse(js.noSpaces))
      .getOrElse(schema)
  }

  /** replace all namespace in the schema with the provided one
    * @param schema
    *   input schema
    * @param ns
    *   new namespace
    * @return
    *
    * schema with the top level namespace replaced.
    *
    * children namespace removed so that the whole schema use the same namespace
    */
  def replaceNamespace(schema: Schema, ns: String): Schema = {
    val replace: Json => Json = Plated.transform[Json] { js =>
      js.asObject match {
        case Some(value) =>
          value.toJson.hcursor.downField("namespace").withFocus(_.mapString(_ => ns)).top.getOrElse(js)
        case None => js
      }
    }

    parser
      .parse(schema.toString(false))
      .toOption
      .map(replace)
      .map(js => (new Schema.Parser).parse(js.noSpaces))
      .getOrElse(schema)
  }

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
      if (gr == null) null
      else {
        val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(baos, null)
        new GenericDatumWriter[GenericRecord](gr.getSchema).write(gr, encoder)
        encoder.flush()

        val decoder: BinaryDecoder = DecoderFactory.get().binaryDecoder(baos.toByteArray, null)
        new GenericDatumReader[GenericData.Record](gr.getSchema, schema).read(null, decoder)
      }
    }(_.close())

  def gr2Jackson(getGenericRecord: => GenericRecord): Try[String] =
    Using(new ByteArrayOutputStream()) { baos =>
      val gr: GenericRecord    = getGenericRecord
      val encoder: JsonEncoder = EncoderFactory.get().jsonEncoder(gr.getSchema, baos)
      new GenericDatumWriter[GenericRecord](gr.getSchema).write(gr, encoder)
      encoder.flush()
      baos.toString(StandardCharsets.UTF_8)
    }(_.close())

  def gr2Circe(getGenericRecord: => GenericRecord): Try[Json] =
    gr2Jackson(getGenericRecord).flatMap(parser.parse(_).toTry)

  def gr2BinAvro(getGenericRecord: => GenericRecord): Try[Array[Byte]] =
    Using(new ByteArrayOutputStream) { baos =>
      val gr: GenericRecord      = getGenericRecord
      val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(baos, null)
      new GenericDatumWriter[GenericRecord](gr.getSchema).write(gr, encoder)
      encoder.flush()
      baos.toByteArray
    }(_.close())

  def jackson2GR(schema: Schema, jackson: String): Try[GenericData.Record] =
    Using(new ByteArrayInputStream(jackson.getBytes)) { bais =>
      val jsonDecoder = DecoderFactory.get().jsonDecoder(schema, bais)
      val datumReader = new GenericDatumReader[GenericData.Record](schema)
      datumReader.read(null, jsonDecoder)
    }(_.close())
}
