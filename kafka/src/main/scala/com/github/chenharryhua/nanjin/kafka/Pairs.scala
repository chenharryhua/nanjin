package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.kafka.record.NJConsumerRecord
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import org.apache.avro.Schema

final case class AvroSchemaPair(key: AvroSchema, value: AvroSchema) {
  val consumerSchema: Schema = NJConsumerRecord.schema(key.rawSchema(), value.rawSchema())
}

final case class ProtobufSchemaPair(key: ProtobufSchema, value: ProtobufSchema)

final case class JsonSchemaPair(key: JsonSchema, value: JsonSchema)

private def missingSchema(message: String): Nothing =
  throw TopicSchemaAbsent(message) // scalafix:ok

/** The JSON schemas registered for a topic's key and value subjects, either of which may be absent. */
final case class OptionalJsonSchemaPair(key: Option[JsonSchema], value: Option[JsonSchema]) {

  /** Both schemas, or `TopicSchemaAbsent` naming the side that is missing. */
  def toSchemaPair: JsonSchemaPair = (key, value) match {
    case (None, None)       => missingSchema("both key and value schema are absent")
    case (None, Some(_))    => missingSchema("key schema is absent")
    case (Some(_), None)    => missingSchema("value schema is absent")
    case (Some(k), Some(v)) => JsonSchemaPair(k, v)
  }
}

/** The Protobuf schemas registered for a topic's key and value subjects, either of which may be absent. */
final case class OptionalProtobufSchemaPair(key: Option[ProtobufSchema], value: Option[ProtobufSchema]) {

  /** Both schemas, or `TopicSchemaAbsent` naming the side that is missing. */
  def toSchemaPair: ProtobufSchemaPair = (key, value) match {
    case (None, None)       => missingSchema("both key and value schema are absent")
    case (None, Some(_))    => missingSchema("key schema is absent")
    case (Some(_), None)    => missingSchema("value schema is absent")
    case (Some(k), Some(v)) => ProtobufSchemaPair(k, v)
  }
}

/** The Avro schemas registered for a topic's key and value subjects, either of which may be absent.
  *
  * Produced by `TopicSchemaRegistry.fetchOptionalAvroSchema` and consumed by the generic connectors, which
  * require both sides and so call `toSchemaPair`.
  */
final case class OptionalAvroSchemaPair(key: Option[AvroSchema], value: Option[AvroSchema]) {

  /** Both schemas, or `TopicSchemaAbsent` naming the side that is missing. */
  def toSchemaPair: AvroSchemaPair = (key, value) match {
    case (None, None)       => missingSchema("both key and value schema are absent")
    case (None, Some(_))    => missingSchema("key schema is absent")
    case (Some(_), None)    => missingSchema("value schema is absent")
    case (Some(k), Some(v)) => AvroSchemaPair(k, v)
  }
}
