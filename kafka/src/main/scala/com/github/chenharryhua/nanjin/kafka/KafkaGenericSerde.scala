package com.github.chenharryhua.nanjin.kafka

import cats.syntax.all.*
import com.github.chenharryhua.nanjin.messages.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.KafkaSerde

import scala.util.{Success, Try}

abstract class KafkaGenericSerde[K, V](keySerde: KafkaSerde[K], valSerde: KafkaSerde[V])
    extends Serializable {

  def deserialize[G[_, _]: NJConsumerMessage](data: G[Array[Byte], Array[Byte]]): G[K, V] =
    data.bimap(keySerde.deserialize, valSerde.deserialize)

  def deserializeKey[G[_, _]: NJConsumerMessage](data: G[Array[Byte], Array[Byte]]): G[K, Array[Byte]] =
    data.bimap(keySerde.deserialize, identity)

  def deserializeValue[G[_, _]: NJConsumerMessage](data: G[Array[Byte], Array[Byte]]): G[Array[Byte], V] =
    data.bimap(identity, valSerde.deserialize)

  def tryDeserializeKeyValue[G[_, _]: NJConsumerMessage](
    data: G[Array[Byte], Array[Byte]]): G[Try[K], Try[V]] =
    data.bimap(keySerde.tryDeserialize, valSerde.tryDeserialize)

  def tryDeserialize[G[_, _]: NJConsumerMessage](data: G[Array[Byte], Array[Byte]]): Try[G[K, V]] =
    data.bitraverse(keySerde.tryDeserialize, valSerde.tryDeserialize)

  def tryDeserializeValue[G[_, _]: NJConsumerMessage](
    data: G[Array[Byte], Array[Byte]]): Try[G[Array[Byte], V]] =
    data.bitraverse(Success(_), valSerde.tryDeserialize)

  def tryDeserializeKey[G[_, _]: NJConsumerMessage](
    data: G[Array[Byte], Array[Byte]]): Try[G[K, Array[Byte]]] =
    data.bitraverse(keySerde.tryDeserialize, Success(_))

  def optionalDeserialize[G[_, _]: NJConsumerMessage](
    data: G[Array[Byte], Array[Byte]]): G[Option[K], Option[V]] =
    tryDeserializeKeyValue(data).bimap(_.toOption, _.toOption)

  def nullableDeserialize[G[_, _]: NJConsumerMessage](data: G[Array[Byte], Array[Byte]]): G[K, V] =
    data.bimap(
      k => keySerde.tryDeserialize(k).getOrElse(null.asInstanceOf[K]),
      v => valSerde.tryDeserialize(v).getOrElse(null.asInstanceOf[V]))

  def toNJConsumerRecord[G[_, _]: NJConsumerMessage](
    gaa: G[Array[Byte], Array[Byte]]): NJConsumerRecord[K, V] =
    NJConsumerRecord(NJConsumerMessage[G].lens.get(optionalDeserialize(gaa))).flatten

  def serializeKey(k: K): Array[Byte] = keySerde.serialize(k)
  def serializeVal(v: V): Array[Byte] = valSerde.serialize(v)

  def serialize[G[_, _]: NJProducerMessage](data: G[K, V]): G[Array[Byte], Array[Byte]] =
    data.bimap(keySerde.serialize, valSerde.serialize)
}
