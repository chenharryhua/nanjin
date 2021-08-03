package com.github.chenharryhua.nanjin.kafka

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.{NJCodec, SerdeOf}
import com.sksamuel.avro4s.SchemaFor
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

final case class RawKeyValueSerdePair[K, V](key: SerdeOf[K], value: SerdeOf[V])

final class RegisteredKeyValueSerdePair[K, V] private[kafka](val keyCodec: NJCodec[K], val valCodec: NJCodec[V]) {
  require(keyCodec.topicName === valCodec.topicName, "key and value codec should have same topic name")

  implicit val keySerde: SerdeOf[K] = keyCodec.cfg.serde
  implicit val valSerde: SerdeOf[V] = valCodec.cfg.serde

  val keySchemaFor: SchemaFor[K] = keySerde.avroCodec.schemaFor
  val valSchemaFor: SchemaFor[V] = valSerde.avroCodec.schemaFor

  val keySerializer: Serializer[K] = keySerde.serializer
  val valSerializer: Serializer[V] = valSerde.serializer

  val keyDeserializer: Deserializer[K] = keySerde.deserializer
  val valDeserializer: Deserializer[V] = valSerde.deserializer
}
