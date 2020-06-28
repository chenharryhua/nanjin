package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.codec.{NJCodec, NJSerde}
import com.sksamuel.avro4s.SchemaFor
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

final class KafkaTopicCodec[K, V] private[kafka] (
  val keyCodec: NJCodec[K],
  val valCodec: NJCodec[V]) {
  require(
    keyCodec.topicName.value === valCodec.topicName.value,
    "key and value codec should have same topic name")

  implicit val keySerde: NJSerde[K] = keyCodec.serde
  implicit val valSerde: NJSerde[V] = valCodec.serde

  val keySchemaFor: SchemaFor[K] = keySerde.schemaFor
  val valSchemaFor: SchemaFor[V] = valSerde.schemaFor

  val keySerializer: Serializer[K] = keySerde.serializer
  val valSerializer: Serializer[V] = valSerde.serializer

  val keyDeserializer: Deserializer[K] = keySerde.deserializer
  val valDeserializer: Deserializer[V] = valSerde.deserializer
}
