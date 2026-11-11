package com.github.chenharryhua.nanjin.kafka.utils

import io.circe.optics.all.*
import io.circe.{jawn, ACursor, Json}
import monocle.function.Plated
import org.apache.avro.{Schema, SchemaCompatibility, SchemaFormatter}

import scala.jdk.CollectionConverters.ListHasAsScala

/** Can `reader` read data written with `writer`?
  *
  * @param reader
  *   the schema doing the reading, typically the newer one
  * @param writer
  *   the schema the data was written with, typically the older one
  * @return
  *   the incompatibilities Avro found, empty when `reader` can read `writer`'s data
  */
def backwardCompatibility(reader: Schema, writer: Schema): List[SchemaCompatibility.Incompatibility] =
  SchemaCompatibility
    .checkReaderWriterCompatibility(reader, writer)
    .getResult
    .getIncompatibilities
    .asScala
    .toList

/** Can data written with `writer` be read by `reader`?
  *
  * The same check as `backwardCompatibility` with the roles swapped, so the argument order follows the
  * direction the data flows: here `writer` is typically the newer schema and `reader` the older one that must
  * keep working.
  *
  * @param writer
  *   the schema the data was written with
  * @param reader
  *   the schema doing the reading
  * @return
  *   the incompatibilities Avro found, empty when `writer`'s data is readable by `reader`
  */
def forwardCompatibility(writer: Schema, reader: Schema): List[SchemaCompatibility.Incompatibility] =
  backwardCompatibility(reader, writer)

private def update(schema: Schema, f: Json => Json): Schema =
  jawn
    .parse(SchemaFormatter.format("json/pretty", schema))
    .toOption
    .map(f)
    .map(js => (new Schema.Parser).parse(js.noSpaces))
    .getOrElse(schema)

private def rewriteSchema(schema: Schema, field: String)(f: ACursor => Option[ACursor]): Schema = {
  val rewrite: Json => Json = Plated.transform[Json] { js =>
    js.asObject match {
      case Some(value) => f(value.toJson.hcursor.downField(field)).flatMap(_.top).getOrElse(js)
      case None        => js
    }
  }

  update(schema, rewrite)
}

/** remove all default fields in the schema
  * @param schema
  *   input schema
  * @return
  *   schema without default fields
  */
def removeDefaultField(schema: Schema): Schema =
  rewriteSchema(schema, "default")(cursor => Some(cursor.delete))

/** remove namespace field from the schema
  *
  * @param schema
  *   input schema
  * @return
  *   schema without namespace
  */
def removeNamespace(schema: Schema): Schema =
  rewriteSchema(schema, "namespace")(cursor => Some(cursor.delete))

/** remove doc field from the schema
  *
  * @param schema
  *   input schema
  * @return
  *   schema without doc
  */
def removeDocField(schema: Schema): Schema =
  rewriteSchema(schema, "doc")(cursor => Some(cursor.delete))

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
def replaceNamespace(schema: Schema, ns: String): Schema =
  rewriteSchema(schema, "namespace")(cursor => Some(cursor.withFocus(_.mapString(_ => ns))))
