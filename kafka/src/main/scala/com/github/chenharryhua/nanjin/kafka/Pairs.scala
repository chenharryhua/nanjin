package com.github.chenharryhua.nanjin.kafka

import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import com.github.chenharryhua.nanjin.kafka.record.NJConsumerRecord
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import org.apache.avro.Schema

import scala.jdk.CollectionConverters.CollectionHasAsScala

final case class AvroSchemaPair(key: AvroSchema, value: AvroSchema) {
  val consumerSchema: Schema = NJConsumerRecord.schema(key.rawSchema(), value.rawSchema())
}

final case class ProtobufSchemaPair(key: ProtobufSchema, value: ProtobufSchema)

final case class JsonSchemaPair(key: JsonSchema, value: JsonSchema)

sealed trait SchemaCompatibility[A] {

  /** Are local schemas backward compatible with broker schemas? */
  def isBackwardCompatible(broker: A): Boolean

  /** Merge schemas for reading (prefer local, fallback to broker) */
  def read(broker: A): A

  /** Merge schemas for writing (prefer broker, fallback to local) */
  def write(broker: A): A
}

final case class OptionalJsonSchemaPair(key: Option[JsonSchema], value: Option[JsonSchema])
    extends SchemaCompatibility[OptionalJsonSchemaPair] {
  override def isBackwardCompatible(broker: OptionalJsonSchemaPair): Boolean = {
    val k = (key, broker.key).traverseN((a, b) => a.isBackwardCompatible(b).asScala.toList).flatten
    val v = (value, broker.value).traverseN((a, b) => a.isBackwardCompatible(b).asScala.toList).flatten
    k.isEmpty && v.isEmpty
  }
  override def read(broker: OptionalJsonSchemaPair): OptionalJsonSchemaPair =
    OptionalJsonSchemaPair(key.orElse(broker.key), value.orElse(broker.value))

  // write prefer broker's schema
  override def write(broker: OptionalJsonSchemaPair): OptionalJsonSchemaPair =
    OptionalJsonSchemaPair(broker.key.orElse(key), broker.value.orElse(value))

  def toSchemaPair: JsonSchemaPair = (key, value) match {
    case (None, None)       => sys.error("both key and value schema are absent")
    case (None, Some(_))    => sys.error("key schema is absent")
    case (Some(_), None)    => sys.error("value schema is absent")
    case (Some(k), Some(v)) => JsonSchemaPair(k, v)
  }
}

final case class OptionalProtobufSchemaPair(key: Option[ProtobufSchema], value: Option[ProtobufSchema])
    extends SchemaCompatibility[OptionalProtobufSchemaPair] {

  override def isBackwardCompatible(broker: OptionalProtobufSchemaPair): Boolean = {
    val k = (key, broker.key).traverseN((a, b) => a.isBackwardCompatible(b).asScala.toList).flatten
    val v = (value, broker.value).traverseN((a, b) => a.isBackwardCompatible(b).asScala.toList).flatten
    k.isEmpty && v.isEmpty
  }
  override def read(broker: OptionalProtobufSchemaPair): OptionalProtobufSchemaPair =
    OptionalProtobufSchemaPair(key.orElse(broker.key), value.orElse(broker.value))

  // write prefer broker's schema
  override def write(broker: OptionalProtobufSchemaPair): OptionalProtobufSchemaPair =
    OptionalProtobufSchemaPair(broker.key.orElse(key), broker.value.orElse(value))

  def toSchemaPair: ProtobufSchemaPair = (key, value) match {
    case (None, None)       => sys.error("both key and value schema are absent")
    case (None, Some(_))    => sys.error("key schema is absent")
    case (Some(_), None)    => sys.error("value schema is absent")
    case (Some(k), Some(v)) => ProtobufSchemaPair(k, v)
  }
}

final private[kafka] case class OptionalAvroSchemaPair(key: Option[AvroSchema], value: Option[AvroSchema])
    extends SchemaCompatibility[OptionalAvroSchemaPair] {

  override def isBackwardCompatible(broker: OptionalAvroSchemaPair): Boolean = {
    val k = (key, broker.key).traverseN((a, b) => a.isBackwardCompatible(b).asScala.toList).flatten
    val v = (value, broker.value).traverseN((a, b) => a.isBackwardCompatible(b).asScala.toList).flatten
    k.isEmpty && v.isEmpty
  }

  override def read(broker: OptionalAvroSchemaPair): OptionalAvroSchemaPair =
    OptionalAvroSchemaPair(key.orElse(broker.key), value.orElse(broker.value))

  // write prefer broker's schema
  override def write(broker: OptionalAvroSchemaPair): OptionalAvroSchemaPair =
    OptionalAvroSchemaPair(broker.key.orElse(key), broker.value.orElse(value))

  def toSchemaPair: AvroSchemaPair = (key, value) match {
    case (None, None)       => sys.error("both key and value schema are absent")
    case (None, Some(_))    => sys.error("key schema is absent")
    case (Some(_), None)    => sys.error("value schema is absent")
    case (Some(k), Some(v)) => AvroSchemaPair(k, v)
  }
}
