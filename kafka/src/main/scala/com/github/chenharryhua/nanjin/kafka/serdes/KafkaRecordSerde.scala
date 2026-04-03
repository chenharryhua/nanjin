package com.github.chenharryhua.nanjin.kafka.serdes

import cats.syntax.bifunctor.given
import cats.syntax.bitraverse.given
import cats.{Bifunctor, Bitraverse}
import com.github.chenharryhua.nanjin.kafka.TopicName
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.util.{Success, Try}

final class KafkaSerde[A] private[kafka] (val serde: Serde[A], topicName: TopicName) {
  private val ser: Serializer[A] = serde.serializer()
  def serialize(a: A): Array[Byte] = ser.serialize(topicName.value, a)

  private val deser: Deserializer[A] = serde.deserializer()
  def deserialize(ab: Array[Byte]): A = deser.deserialize(topicName.value, ab)
}

case object DeserializeNull extends Exception("deserialize null")

abstract class KafkaRecordSerde[K, V] private[kafka] (keySerde: KafkaSerde[K], valSerde: KafkaSerde[V]) {

  def deserialize[G[_, _]: Bifunctor](data: G[Array[Byte], Array[Byte]]): G[K, V] =
    data.bimap(keySerde.deserialize, valSerde.deserialize)

  def deserializeKey[G[_, _]: Bifunctor](data: G[Array[Byte], Array[Byte]]): G[Option[K], Array[Byte]] =
    data.bimap(Option(_).map(keySerde.deserialize), identity)

  def deserializeValue[G[_, _]: Bifunctor](data: G[Array[Byte], Array[Byte]]): G[Array[Byte], Option[V]] =
    data.bimap(identity, Option(_).map(valSerde.deserialize))

  def tryDeserializeKeyValue[G[_, _]: Bifunctor](data: G[Array[Byte], Array[Byte]]): G[Try[K], Try[V]] =
    data.bimap(
      nk => Option(nk).toRight(DeserializeNull).toTry.flatMap(k => Try(keySerde.deserialize(k))),
      nv => Option(nv).toRight(DeserializeNull).toTry.flatMap(v => Try(valSerde.deserialize(v)))
    )

  def tryDeserialize[G[_, _]: Bitraverse](data: G[Array[Byte], Array[Byte]]): Try[G[K, V]] =
    tryDeserializeKeyValue(data).bitraverse(identity, identity)

  def tryDeserializeValue[G[_, _]: Bitraverse](data: G[Array[Byte], Array[Byte]]): Try[G[Array[Byte], V]] =
    data.bitraverse(
      Success(_),
      nv => Option(nv).toRight(DeserializeNull).toTry.flatMap(v => Try(valSerde.deserialize(v)))
    )

  def tryDeserializeKey[G[_, _]: Bitraverse](data: G[Array[Byte], Array[Byte]]): Try[G[K, Array[Byte]]] =
    data.bitraverse(
      nk => Option(nk).toRight(DeserializeNull).toTry.flatMap(k => Try(keySerde.deserialize(k))),
      Success(_)
    )

  def optionalDeserialize[G[_, _]: Bitraverse](data: G[Array[Byte], Array[Byte]]): G[Option[K], Option[V]] =
    data.bimap(
      nk => Option(nk).flatMap(k => Try(keySerde.deserialize(k)).toOption),
      nv => Option(nv).flatMap(v => Try(valSerde.deserialize(v)).toOption)
    )

  /*
   * Serialize
   */

  def serializeKey(k: K): Array[Byte] = keySerde.serialize(k)
  def serializeVal(v: V): Array[Byte] = valSerde.serialize(v)

  def serialize[G[_, _]: Bifunctor](data: G[K, V]): G[Array[Byte], Array[Byte]] =
    data.bimap(serializeKey, serializeVal)
}
