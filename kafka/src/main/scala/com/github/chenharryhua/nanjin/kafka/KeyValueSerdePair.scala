package com.github.chenharryhua.nanjin.kafka

import cats.syntax.eq.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.messages.kafka.codec.{NJCodec, SerdeOf}
import com.sksamuel.avro4s.SchemaFor
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

final case class RawKeyValueSerdePair[K, V](keySerde: SerdeOf[K], valSerde: SerdeOf[V]) {
  def register(srs: SchemaRegistrySettings, name: TopicName): KeyValueCodecPair[K, V] =
    KeyValueCodecPair(
      keySerde.asKey(srs.config).codec(name.value),
      valSerde.asValue(srs.config).codec(name.value))
}

final case class KeyValueCodecPair[K, V](keyCodec: NJCodec[K], valCodec: NJCodec[V]) {
  require(keyCodec.name === valCodec.name, "key and value codec should have same topic name")

  val keySerde: SerdeOf[K] = keyCodec.registered.serde
  val valSerde: SerdeOf[V] = valCodec.registered.serde

  val keySchemaFor: SchemaFor[K] = keySerde.avroCodec.schemaFor
  val valSchemaFor: SchemaFor[V] = valSerde.avroCodec.schemaFor

  val keySerializer: Serializer[K] = keySerde.serializer
  val valSerializer: Serializer[V] = valSerde.serializer

  val keyDeserializer: Deserializer[K] = keySerde.deserializer
  val valDeserializer: Deserializer[V] = valSerde.deserializer

  def asRegisteredKeyValueSerdePair: RegisteredKeyValueSerdePair[K, V] =
    RegisteredKeyValueSerdePair(keySerde, valSerde)
}

final case class RegisteredKeyValueSerdePair[K, V](keySerde: Serde[K], valSerde: Serde[V])
