package com.github.chenharryhua.nanjin.kafka

import cats.Show
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.{KafkaSerde, SerdeOf}
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import org.apache.avro.Schema

import scala.jdk.CollectionConverters.ListHasAsScala

final private[kafka] case class RawKeyValueSerdePair[K, V](key: SerdeOf[K], value: SerdeOf[V]) {
  def register(srs: SchemaRegistrySettings, name: TopicName): KeyValueSerdePair[K, V] =
    KeyValueSerdePair(key.asKey(srs.config).topic(name.value), value.asValue(srs.config).topic(name.value))

  def withSchema(pair: AvroSchemaPair): RawKeyValueSerdePair[K, V] =
    RawKeyValueSerdePair(key.withSchema(pair.key), value.withSchema(pair.value))
}

final private[kafka] case class KeyValueSerdePair[K, V](key: KafkaSerde[K], value: KafkaSerde[V])

final case class AvroSchemaPair(key: Schema, value: Schema) {
  val consumerRecordSchema: Schema = NJConsumerRecord.schema(key, value)

  def isBackwardCompatible(other: AvroSchemaPair): Boolean = {
    val k = new AvroSchema(key).isBackwardCompatible(new AvroSchema(other.key)).asScala.toList
    val v = new AvroSchema(value).isBackwardCompatible(new AvroSchema(other.value)).asScala.toList
    k.isEmpty && v.isEmpty
  }

  def isIdentical(other: AvroSchemaPair): Boolean =
    key.equals(other.key) && value.equals(other.value)

}

object AvroSchemaPair {
  implicit val showAvroSchemaPair: Show[AvroSchemaPair] = _.consumerRecordSchema.toString
}
