package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.messages.kafka.codec.*
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.google.protobuf.Descriptors
import fs2.kafka.*
import io.confluent.kafka.schemaregistry.json.JsonSchema
import org.apache.avro.{Schema, SchemaCompatibility}

sealed trait SerdePair[K, V] extends Serializable {
  protected def key: RegisterSerde[K]
  protected def value: RegisterSerde[V]

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
      Serializer.delegate(key.asKey(srs.config).serde.serializer()),
      Serializer.delegate(value.asValue(srs.config).serde.serializer())
    ).withProperties(producerSettings.properties)
}

final case class AvroForPair[K, V](key: AvroFor[K], value: AvroFor[V]) extends SerdePair[K, V] {
  val optionalAvroSchemaPair: OptionalAvroSchemaPair = OptionalAvroSchemaPair(key.schema, value.schema)
}

final case class ProtobufForPair[K, V](key: ProtobufFor[K], value: ProtobufFor[V]) extends SerdePair[K, V]

final case class JsonSchemaForPair[K, V](key: JsonFor[K], value: JsonFor[V]) extends SerdePair[K, V]

final case class AvroSchemaPair(key: Schema, value: Schema) {
  val consumerSchema: Schema = NJConsumerRecord.schema(key, value)
  val producerSchema: Schema = NJProducerRecord.schema(key, value)

  def backward(other: AvroSchemaPair): List[SchemaCompatibility.Incompatibility] =
    backwardCompatibility(consumerSchema, other.consumerSchema)
  def forward(other: AvroSchemaPair): List[SchemaCompatibility.Incompatibility] =
    forwardCompatibility(consumerSchema, other.consumerSchema)

  def isFullCompatible(other: AvroSchemaPair): Boolean =
    backward(other).isEmpty && forward(other).isEmpty

  def isIdentical(other: AvroSchemaPair): Boolean =
    key.equals(other.key) && value.equals(other.value)
}

final case class ProtobufDescriptorPair(key: Descriptors.Descriptor, value: Descriptors.Descriptor)
final case class JsonSchemaPair(key: JsonSchema, value: JsonSchema)

final private[kafka] case class OptionalAvroSchemaPair(key: Option[Schema], value: Option[Schema]) {
  def read(broker: OptionalAvroSchemaPair): OptionalAvroSchemaPair =
    OptionalAvroSchemaPair(key.orElse(broker.key), value.orElse(broker.value))

  // write prefer broker's schema
  def write(broker: OptionalAvroSchemaPair): OptionalAvroSchemaPair =
    OptionalAvroSchemaPair(broker.key.orElse(key), broker.value.orElse(value))

  def toPair: AvroSchemaPair = (key, value) match {
    case (None, None)       => sys.error("both key and value schema are absent")
    case (None, Some(_))    => sys.error("key schema is absent")
    case (Some(_), None)    => sys.error("value schema is absent")
    case (Some(k), Some(v)) => AvroSchemaPair(k, v)
  }
}
