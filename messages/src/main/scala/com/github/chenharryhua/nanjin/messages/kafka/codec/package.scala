package com.github.chenharryhua.nanjin.messages.kafka

import io.circe.{parser, Json}
import monocle.function.Plated
import org.apache.avro.{Schema, SchemaCompatibility}
import com.github.chenharryhua.nanjin.common.optics.jsonPlated

import scala.jdk.CollectionConverters.ListHasAsScala

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

}
