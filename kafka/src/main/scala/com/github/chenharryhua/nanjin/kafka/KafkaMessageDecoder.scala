package com.github.chenharryhua.nanjin.kafka

import akka.kafka.ConsumerMessage.{CommittableMessage => AkkaCommittableMessage}
import cats.Bitraverse
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.CodecException._
import fs2.kafka.{CommittableMessage => Fs2CommittableMessage}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer

import scala.util.{Failure, Success, Try}

abstract class KafkaMessageDecoder[F[_, _]: Bitraverse, K, V](
  topicName: KafkaTopicName,
  keyDeser: Deserializer[K],
  valueDeser: Deserializer[V])
    extends Serializable {

  private def keyDecode(data: Array[Byte]): Try[K] =
    Option(data)
      .traverse(d => Try(keyDeser.deserialize(topicName.value, d)))
      .flatMap(_.fold[Try[K]](Failure(DecodingNullKeyException(topicName.value)))(Success(_)))

  private def valueDecode(data: Array[Byte]): Try[V] =
    Option(data)
      .traverse(d => Try(valueDeser.deserialize(topicName.value, d)))
      .flatMap(_.fold[Try[V]](Failure(DecodingNullValueException(topicName.value)))(Success(_)))

  final def decodeMessage(data: F[Array[Byte], Array[Byte]]): F[Try[K], Try[V]] =
    data.bimap(keyDecode, valueDecode)

  final def decodeKey(data: F[Array[Byte], Array[Byte]]): Try[F[K, Array[Byte]]] =
    data.bitraverse(keyDecode, Success(_))

  final def decodeValue(data: F[Array[Byte], Array[Byte]]): Try[F[Array[Byte], V]] =
    data.bitraverse(Success(_), valueDecode)

  final def decodeBoth(data: F[Array[Byte], Array[Byte]]): Try[F[K, V]] =
    data.bitraverse(keyDecode, valueDecode)
}

object decoders extends Fs2MessageBitraverse with AkkaMessageBitraverse {

  final class ConsumerRecordDecoder[K, V](
    topicName: KafkaTopicName,
    keyDeser: Deserializer[K],
    valueDeser: Deserializer[V]
  ) extends KafkaMessageDecoder[ConsumerRecord, K, V](topicName, keyDeser, valueDeser)

  trait Fs2MessageDecoder[F[_], K, V]
      extends KafkaMessageDecoder[Fs2CommittableMessage[F, ?, ?], K, V]

  trait AkkaMessageDecoder[K, V] extends KafkaMessageDecoder[AkkaCommittableMessage, K, V]
}
