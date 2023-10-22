package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.{
  backwardCompatibility,
  forwardCompatibility,
  KafkaSerde,
  SerdeOf
}
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import org.apache.avro.{Schema, SchemaCompatibility}

final private[kafka] case class RawKeyValueSerdePair[K, V](key: SerdeOf[K], value: SerdeOf[V]) {
  def register(srs: SchemaRegistrySettings, name: TopicName): KeyValueSerdePair[K, V] =
    KeyValueSerdePair(key.asKey(srs.config).topic(name), value.asValue(srs.config).topic(name))

}

final private[kafka] case class KeyValueSerdePair[K, V](key: KafkaSerde[K], value: KafkaSerde[V])

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
