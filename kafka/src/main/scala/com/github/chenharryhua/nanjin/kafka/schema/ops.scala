package com.github.chenharryhua.nanjin.kafka.schema

import io.circe.optics.all.*
import io.circe.{jawn, ACursor, Json}
import monocle.function.Plated
import org.apache.avro.{Schema, SchemaCompatibility, SchemaFormatter}

import scala.jdk.CollectionConverters.ListHasAsScala

def backwardCompatibility(a: Schema, b: Schema): List[SchemaCompatibility.Incompatibility] =
  SchemaCompatibility.checkReaderWriterCompatibility(a, b).getResult.getIncompatibilities.asScala.toList

def forwardCompatibility(a: Schema, b: Schema): List[SchemaCompatibility.Incompatibility] =
  backwardCompatibility(b, a)

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
