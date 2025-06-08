package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.{
  backwardCompatibility,
  forwardCompatibility,
  AvroCodecOf,
  KafkaSerde
}
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import org.apache.avro.{Schema, SchemaCompatibility}

final case class AvroCodecPair[K, V](key: AvroCodecOf[K], value: AvroCodecOf[V]) {
  def register(srs: SchemaRegistrySettings, name: TopicName): SerdePair[K, V] =
    SerdePair(name, key.asKey(srs.config).withTopic(name), value.asValue(srs.config).withTopic(name))
}

final case class SerdePair[K, V](name: TopicName, key: KafkaSerde[K], value: KafkaSerde[V])

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
