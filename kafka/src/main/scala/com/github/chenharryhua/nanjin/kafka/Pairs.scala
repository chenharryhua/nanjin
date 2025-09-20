package com.github.chenharryhua.nanjin.kafka

import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.*
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.google.protobuf.Descriptors
import com.sksamuel.avro4s.{Record, RecordFormat}
import fs2.kafka.*
import io.confluent.kafka.schemaregistry.json.JsonSchema
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.{Schema, SchemaCompatibility}
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord as JavaProducerRecord

sealed trait SerdePair[K, V] {
  protected def key: RegisterSerde[K]
  protected def value: RegisterSerde[V]

  final def register(srs: SchemaRegistrySettings, topicName: TopicName): TopicSerde[K, V] =
    TopicSerde(
      topicName,
      key.asKey(srs.config).withTopic(topicName),
      value.asValue(srs.config).withTopic(topicName))

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

final case class TopicSerde[K, V](topicName: TopicName, key: KafkaSerde[K], value: KafkaSerde[V])
    extends KafkaGenericSerde(key, value)

final case class AvroPair[K, V](key: AvroFor[K], value: AvroFor[V]) extends SerdePair[K, V] {

  val schemaPair: AvroSchemaPair =
    AvroSchemaPair(key.avroCodec.schema, value.avroCodec.schema)

  final class ConsumerFormat private[AvroPair] (rf: RecordFormat[NJConsumerRecord[K, V]])
      extends Serializable {
    val codec: AvroCodec[NJConsumerRecord[K, V]] =
      NJConsumerRecord.avroCodec(key.avroCodec, value.avroCodec)

    def toRecord(nj: NJConsumerRecord[K, V]): Record = rf.to(nj)
    def toRecord(cr: ConsumerRecord[K, V]): Record = toRecord(NJConsumerRecord(cr))
    def toRecord(jcr: JavaConsumerRecord[K, V]): Record = toRecord(NJConsumerRecord(jcr))

    def fromRecord(gr: IndexedRecord): NJConsumerRecord[K, V] = rf.from(gr)
  }

  final class ProducerFormat private[AvroPair] (rf: RecordFormat[NJProducerRecord[K, V]])
      extends Serializable {
    val codec: AvroCodec[NJProducerRecord[K, V]] =
      NJProducerRecord.avroCodec(key.avroCodec, value.avroCodec)

    def toRecord(nj: NJProducerRecord[K, V]): Record = rf.to(nj)
    def toRecord(pr: ProducerRecord[K, V]): Record = toRecord(NJProducerRecord(pr))
    def toRecord(jpr: JavaProducerRecord[K, V]): Record = toRecord(NJProducerRecord(jpr))

    def fromRecord(gr: IndexedRecord): NJProducerRecord[K, V] = rf.from(gr)
  }

  val consumerFormat: ConsumerFormat = {
    val consumerCodec: AvroCodec[NJConsumerRecord[K, V]] =
      NJConsumerRecord.avroCodec(key.avroCodec, value.avroCodec)
    new ConsumerFormat(RecordFormat(consumerCodec, consumerCodec))
  }

  val producerFormat: ProducerFormat = {
    val producerCodec: AvroCodec[NJProducerRecord[K, V]] =
      NJProducerRecord.avroCodec(key.avroCodec, value.avroCodec)
    new ProducerFormat(RecordFormat(producerCodec, producerCodec))
  }
}

final case class ProtobufPair[K, V](key: ProtobufFor[K], value: ProtobufFor[V]) extends SerdePair[K, V]

final case class JsonPair[K, V](key: JsonFor[K], value: JsonFor[V]) extends SerdePair[K, V]

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

final class OptionalAvroSchemaPair private[kafka] (key: Option[Schema], value: Option[Schema]) {
  def withKeyIfAbsent(schema: Schema): OptionalAvroSchemaPair =
    new OptionalAvroSchemaPair(key.orElse(Some(schema)), value)
  def withValIfAbsent(schema: Schema): OptionalAvroSchemaPair =
    new OptionalAvroSchemaPair(key, value.orElse(Some(schema)))

  def withKeyReplaced(schema: Schema): OptionalAvroSchemaPair =
    new OptionalAvroSchemaPair(Some(schema), value)
  def withValReplaced(schema: Schema): OptionalAvroSchemaPair =
    new OptionalAvroSchemaPair(key, Some(schema))

  def withNullKey: OptionalAvroSchemaPair = withKeyReplaced(Schema.create(Schema.Type.NULL))
  def withNullVal: OptionalAvroSchemaPair = withValReplaced(Schema.create(Schema.Type.NULL))

  private[kafka] def toPair: AvroSchemaPair = (key, value) match {
    case (None, None)       => sys.error("both key and value schema are absent")
    case (None, Some(_))    => sys.error("key schema is absent")
    case (Some(_), None)    => sys.error("value schema is absent")
    case (Some(k), Some(v)) => AvroSchemaPair(k, v)
  }
}
