package com.github.chenharryhua.nanjin.kafka

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.{NJCodec, SerdeOf}
import com.sksamuel.avro4s.SchemaFor
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

final case class RawKeyValueSerdePair[K, V](key: SerdeOf[K], value: SerdeOf[V])

final case class RegisteredKeyValueSerdePair[K, V](key: NJCodec[K], value: NJCodec[V]) {
  require(key.topicName === value.topicName, "key and value codec should have same topic name")

  implicit val keySerde: SerdeOf[K] = key.cfg.serde
  implicit val valSerde: SerdeOf[V] = value.cfg.serde

  val keySchemaFor: SchemaFor[K] = keySerde.avroCodec.schemaFor
  val valSchemaFor: SchemaFor[V] = valSerde.avroCodec.schemaFor

  val keySerializer: Serializer[K] = keySerde.serializer
  val valSerializer: Serializer[V] = valSerde.serializer

  val keyDeserializer: Deserializer[K] = keySerde.deserializer
  val valDeserializer: Deserializer[V] = valSerde.deserializer
}
