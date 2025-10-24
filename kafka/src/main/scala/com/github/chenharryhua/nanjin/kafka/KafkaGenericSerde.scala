package com.github.chenharryhua.nanjin.kafka

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.KafkaSerde

import scala.util.{Success, Try}

abstract class KafkaGenericSerde[K, V] private[kafka] (keySerde: KafkaSerde[K], valSerde: KafkaSerde[V]) {

  def deserialize[G[_, _]: NJConsumerMessage](data: G[Array[Byte], Array[Byte]]): G[K, V] =
    data.bimap(keySerde.deserialize, valSerde.deserialize)

  def deserializeKey[G[_, _]: NJConsumerMessage](data: G[Array[Byte], Array[Byte]]): G[K, Array[Byte]] =
    data.bimap(keySerde.deserialize, identity)

  def deserializeValue[G[_, _]: NJConsumerMessage](data: G[Array[Byte], Array[Byte]]): G[Array[Byte], V] =
    data.bimap(identity, valSerde.deserialize)

  def tryDeserializeKeyValue[G[_, _]: NJConsumerMessage](
    data: G[Array[Byte], Array[Byte]]): G[Try[K], Try[V]] =
    data.bimap(k => Try(keySerde.deserialize(k)), v => Try(valSerde.deserialize(v)))

  def tryDeserialize[G[_, _]: NJConsumerMessage](data: G[Array[Byte], Array[Byte]]): Try[G[K, V]] =
    data.bitraverse(k => Try(keySerde.deserialize(k)), v => Try(valSerde.deserialize(v)))

  def tryDeserializeValue[G[_, _]: NJConsumerMessage](
    data: G[Array[Byte], Array[Byte]]): Try[G[Array[Byte], V]] =
    data.bitraverse(Success(_), v => Try(valSerde.deserialize(v)))

  def tryDeserializeKey[G[_, _]: NJConsumerMessage](
    data: G[Array[Byte], Array[Byte]]): Try[G[K, Array[Byte]]] =
    data.bitraverse(k => Try(keySerde.deserialize(k)), Success(_))

  def optionalDeserialize[G[_, _]: NJConsumerMessage](
    data: G[Array[Byte], Array[Byte]]): G[Option[K], Option[V]] =
    tryDeserializeKeyValue(data).bimap(_.toOption.flatMap(Option(_)), _.toOption.flatMap(Option(_)))

  def toNJConsumerRecord[G[_, _]: NJConsumerMessage](
    gaa: G[Array[Byte], Array[Byte]]): NJConsumerRecord[K, V] =
    NJConsumerRecord(NJConsumerMessage[G].lens.get(optionalDeserialize(gaa))).flatten

  def serializeKey(k: K): Array[Byte] = keySerde.serialize(k)
  def serializeVal(v: V): Array[Byte] = valSerde.serialize(v)

  def serialize[G[_, _]: NJProducerMessage](data: G[K, V]): G[Array[Byte], Array[Byte]] =
    data.bimap(serializeKey, serializeVal)
}
