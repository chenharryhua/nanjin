package com.github.chenharryhua.nanjin.kafka

import akka.kafka.ConsumerMessage.{CommittableMessage => AkkaCommittableMessage}
import cats.Bifunctor
import cats.implicits._
import fs2.kafka.{CommittableMessage => Fs2CommittableMessage}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde}

import scala.util.Try
import scala.util.Failure
import scala.util.Success

sealed abstract class KafkaMessageDecoder[F[_, _]: Bifunctor, K, V](
  topicName: KafkaTopicName,
  keySerde: Serde[K],
  valueSerde: Serde[V])
    extends Serializable {

  protected def key[A, B](data: F[A, B]): A
  protected def value[A, B](data: F[A, B]): B

  private val keyDeser: Deserializer[K]   = keySerde.deserializer
  private val valueDeser: Deserializer[V] = valueSerde.deserializer

  private def keyDecode(data: Array[Byte]): Try[K] =
    Option(data)
      .traverse(d => Try(keyDeser.deserialize(topicName.value, d)))
      .flatMap(_.fold[Try[K]](Failure(DecodingNullException(topicName.value)))(Success(_)))

  private def valueDecode(data: Array[Byte]): Try[V] =
    Option(data)
      .traverse(d => Try(valueDeser.deserialize(topicName.value, d)))
      .flatMap(_.fold[Try[V]](Failure(DecodingNullException(topicName.value)))(Success(_)))

  final def decodeMessage(data: F[Array[Byte], Array[Byte]]): F[Try[K], Try[V]] =
    data.bimap(keyDecode, valueDecode)

  final def decodeKey(data: F[Array[Byte], Array[Byte]]): Try[F[K, Array[Byte]]] =
    key(data.bimap(keyDecode, identity)).map(k => data.bimap(_ => k, identity))

  final def decodeValue(data: F[Array[Byte], Array[Byte]]): Try[F[Array[Byte], V]] =
    value(data.bimap(identity, valueDecode)).map(v => data.bimap(identity, _ => v))

  final def decodeBoth(data: F[Array[Byte], Array[Byte]]): Try[F[K, V]] = {
    val m = decodeMessage(data)
    (key(m), value(m)).mapN((k, v) => data.bimap(_ => k, _ => v))
  }
}

object decoders extends Fs2MessageBifunctor with AkkaMessageBifunctor {

  final class ConsumerRecordDecoder[K, V](
    topicName: KafkaTopicName,
    keySerde: Serde[K],
    valueSerde: Serde[V]
  ) extends KafkaMessageDecoder[ConsumerRecord, K, V](topicName, keySerde, valueSerde) {
    override protected def key[A, B](data: ConsumerRecord[A, B]): A   = data.key
    override protected def value[A, B](data: ConsumerRecord[A, B]): B = data.value
  }

  trait Fs2MessageDecoder[F[_], K, V]
      extends KafkaMessageDecoder[Fs2CommittableMessage[F, ?, ?], K, V] {
    final override protected def key[A, B](data: Fs2CommittableMessage[F, A, B]): A =
      data.record.key
    final override protected def value[A, B](data: Fs2CommittableMessage[F, A, B]): B =
      data.record.value
  }

  trait AkkaMessageDecoder[K, V] extends KafkaMessageDecoder[AkkaCommittableMessage, K, V] {
    final override protected def key[A, B](data: AkkaCommittableMessage[A, B]): A =
      data.record.key
    final override protected def value[A, B](data: AkkaCommittableMessage[A, B]): B =
      data.record.value
  }
}
