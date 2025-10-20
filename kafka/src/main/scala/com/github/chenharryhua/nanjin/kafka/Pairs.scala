package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import cats.implicits.catsSyntaxTuple2Semigroupal
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.*
import fs2.kafka.*
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import org.apache.avro.Schema

import scala.jdk.CollectionConverters.CollectionHasAsScala

sealed trait SerdePair[K, V] {
  protected def key: UnregisteredSerde[K]
  protected def value: UnregisteredSerde[V]

  final def consumerSettings[F[_]: Sync](
    srs: SchemaRegistrySettings,
    consumerSettings: KafkaConsumerSettings): ConsumerSettings[F, K, V] =
    ConsumerSettings[F, K, V](
      Deserializer.delegate[F, K](key.asKey(srs.config).serde.deserializer()),
      Deserializer.delegate[F, V](value.asValue(srs.config).serde.deserializer())
    ).withProperties(consumerSettings.properties)

  final def producerSettings[F[_]: Sync](
    srs: SchemaRegistrySettings,
    producerSettings: KafkaProducerSettings): ProducerSettings[F, K, V] =
    ProducerSettings[F, K, V](
      Serializer.delegate[F, K](key.asKey(srs.config).serde.serializer()),
      Serializer.delegate[F, V](value.asValue(srs.config).serde.serializer())
    ).withProperties(producerSettings.properties)
}

final case class AvroForPair[K, V](key: AvroFor[K], value: AvroFor[V]) extends SerdePair[K, V] {
  val optionalSchemaPair: OptionalAvroSchemaPair =
    OptionalAvroSchemaPair(key.schema, value.schema)
}

final case class ProtoForPair[K, V](key: ProtoFor[K], value: ProtoFor[V]) extends SerdePair[K, V] {
  val optionalSchemaPair: OptionalProtobufSchemaPair =
    OptionalProtobufSchemaPair(key.protobufSchema, value.protobufSchema)
}

final case class JsonForPair[K, V](key: JsonFor[K], value: JsonFor[V]) extends SerdePair[K, V] {
  val optionalSchemaPair: OptionalJsonSchemaPair =
    OptionalJsonSchemaPair(key.jsonSchema, value.jsonSchema)
}

final case class AvroSchemaPair(key: AvroSchema, value: AvroSchema) {
  val consumerSchema: Schema = NJConsumerRecord.schema(key.rawSchema(), value.rawSchema())
}

final case class ProtobufSchemaPair(key: ProtobufSchema, value: ProtobufSchema)

final case class JsonSchemaPair(key: JsonSchema, value: JsonSchema)

/*
 * optional
 */
sealed trait CheckBackwardCompatibility[A] {
  def isBackwardCompatible(broker: A): Boolean
  def read(broker: A): A
  def write(broker: A): A
}

final case class OptionalJsonSchemaPair(key: Option[JsonSchema], value: Option[JsonSchema])
    extends CheckBackwardCompatibility[OptionalJsonSchemaPair] {
  override def isBackwardCompatible(broker: OptionalJsonSchemaPair): Boolean = {
    val k = (key, broker.key).traverseN((a, b) => a.isBackwardCompatible(b).asScala.toList).flatten
    val v = (value, broker.value).traverseN((a, b) => a.isBackwardCompatible(b).asScala.toList).flatten
    k.isEmpty & v.isEmpty
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
    extends CheckBackwardCompatibility[OptionalProtobufSchemaPair] {

  override def isBackwardCompatible(broker: OptionalProtobufSchemaPair): Boolean = {
    val k = (key, broker.key).traverseN((a, b) => a.isBackwardCompatible(b).asScala.toList).flatten
    val v = (value, broker.value).traverseN((a, b) => a.isBackwardCompatible(b).asScala.toList).flatten
    k.isEmpty & v.isEmpty
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
    extends CheckBackwardCompatibility[OptionalAvroSchemaPair] {

  override def isBackwardCompatible(broker: OptionalAvroSchemaPair): Boolean = {
    val k = (key, broker.key).traverseN((a, b) => a.isBackwardCompatible(b).asScala.toList).flatten
    val v = (value, broker.value).traverseN((a, b) => a.isBackwardCompatible(b).asScala.toList).flatten
    k.isEmpty & v.isEmpty
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
