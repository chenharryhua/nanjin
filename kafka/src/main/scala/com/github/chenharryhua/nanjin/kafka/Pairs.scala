package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.*
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.google.protobuf.Descriptors
import com.sksamuel.avro4s.{Record, RecordFormat}
import fs2.kafka.{ConsumerRecord, ProducerRecord}
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.{Schema, SchemaCompatibility}
import org.apache.kafka.clients.consumer.ConsumerRecord as JavaConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord as JavaProducerRecord
import scalapb.GeneratedMessage

final case class TopicSerde[K, V](name: TopicName, key: KafkaSerde[K], value: KafkaSerde[V])

final case class AvroPair[K, V](key: AvroFor[K], value: AvroFor[V]) {
  def register(srs: SchemaRegistrySettings, name: TopicName): TopicSerde[K, V] =
    TopicSerde(name, key.asKey(srs.config).withTopic(name), value.asValue(srs.config).withTopic(name))

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

final case class ProtobufPair[K <: GeneratedMessage, V <: GeneratedMessage](
  key: ProtobufFor[K],
  value: ProtobufFor[V]) {
  def register(srs: SchemaRegistrySettings, name: TopicName): TopicSerde[K, V] =
    TopicSerde(name, key.asKey(srs.config).withTopic(name), value.asValue(srs.config).withTopic(name))
}

final case class JsonPair[K, V](key: JsonFor[K], value: JsonFor[V]) {
  def register(srs: SchemaRegistrySettings, name: TopicName): TopicSerde[K, V] =
    TopicSerde(name, key.asKey(srs.config).withTopic(name), value.asValue(srs.config).withTopic(name))
}

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
