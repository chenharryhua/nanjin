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
  def register(srs: SchemaRegistrySettings, name: TopicName): KafkaTopic[K, V] =
    KafkaTopic(name, key.asKey(srs.config).withTopic(name), value.asValue(srs.config).withTopic(name))
}

final case class KafkaTopic[K, V](name: TopicName, key: KafkaSerde[K], value: KafkaSerde[V])

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

final class OptionalAvroSchemaPair(key: Option[Schema], value: Option[Schema]) {
  def withKeySchema(schema: Schema): OptionalAvroSchemaPair =
    new OptionalAvroSchemaPair(Some(schema), value)
  def withValSchema(schema: Schema): OptionalAvroSchemaPair =
    new OptionalAvroSchemaPair(key, Some(schema))

  private[kafka] def toPair: AvroSchemaPair = (key, value) match {
    case (None, None)       => throw new Exception("both key and value schema are absent")
    case (None, Some(_))    => throw new Exception("key schema is absent")
    case (Some(_), None)    => throw new Exception("value schema is absent")
    case (Some(k), Some(v)) => AvroSchemaPair(k, v)
  }
}
