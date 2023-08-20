package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.syntax.eq.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.{KafkaSerde, SerdeOf}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.Serde

final case class RawKeyValueSerdePair[K, V](key: SerdeOf[K], value: SerdeOf[V]) {
  def register(srs: SchemaRegistrySettings, name: TopicName): KeyValueSerdePair[K, V] =
    KeyValueSerdePair(key.asKey(srs.config).topic(name.value), value.asValue(srs.config).topic(name.value))

  def withSchema(pair: AvroSchemaPair): RawKeyValueSerdePair[K, V] =
    RawKeyValueSerdePair(key.withSchema(pair.key), value.withSchema(pair.value))
}

final case class KeyValueSerdePair[K, V](key: KafkaSerde[K], value: KafkaSerde[V]) {
  require(key.name === value.name, "key and value codec should have same topic name")

  def asRegisteredKeyValueSerdePair: RegisteredKeyValueSerdePair[K, V] =
    RegisteredKeyValueSerdePair(key.serde, value.serde)
}

final case class RegisteredKeyValueSerdePair[K, V](key: Serde[K], value: Serde[V])

final case class AvroSchemaPair(key: Schema, value: Schema) {
  val consumerRecordSchema: Schema = NJConsumerRecord.schema(key, value)
}

object AvroSchemaPair {
  implicit val showAvroSchemaPair: Show[AvroSchemaPair] = _.consumerRecordSchema.toString
}
