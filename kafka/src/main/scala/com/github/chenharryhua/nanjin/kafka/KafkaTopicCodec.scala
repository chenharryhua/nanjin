package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.kafka.codec.{NJCodec, NJSerde}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import cats.implicits._

final class KafkaTopicCodec[K, V] private[kafka] (
  val keyCodec: NJCodec[K],
  val valCodec: NJCodec[V]) {
  require(
    keyCodec.topicName.value === valCodec.topicName.value,
    "key and value codec should have same topic name")

  implicit val keySerde: NJSerde[K] = keyCodec.serde
  implicit val valSerde: NJSerde[V] = valCodec.serde

  val keySchema: Schema = keySerde.schema
  val valSchema: Schema = valSerde.schema

  val keySerializer: Serializer[K] = keySerde.serializer
  val valSerializer: Serializer[V] = valSerde.serializer

  val keyDeserializer: Deserializer[K] = keySerde.deserializer
  val valDeserializer: Deserializer[V] = valSerde.deserializer
}
