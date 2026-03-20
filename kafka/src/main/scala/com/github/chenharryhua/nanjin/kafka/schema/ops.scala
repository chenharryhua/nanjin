package com.github.chenharryhua.nanjin.kafka.schema

import io.circe.optics.all.*
import io.circe.{jawn, Json}
import monocle.function.Plated
import org.apache.avro.{Schema, SchemaFormatter}

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

  jawn
    .parse(SchemaFormatter.format("json/pretty", schema))
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

  jawn
    .parse(SchemaFormatter.format("json/pretty", schema))
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

  jawn
    .parse(SchemaFormatter.format("json/pretty", schema))
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

  jawn
    .parse(SchemaFormatter.format("json/pretty", schema))
    .toOption
    .map(replace)
    .map(js => (new Schema.Parser).parse(js.noSpaces))
    .getOrElse(schema)
}
