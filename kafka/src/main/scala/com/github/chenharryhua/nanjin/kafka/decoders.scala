package com.github.chenharryhua.nanjin.kafka

import akka.kafka.ConsumerMessage.{CommittableMessage => AkkaCommittableMessage}
import cats.Bitraverse
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.CodecException._
import fs2.kafka.{CommittableMessage => Fs2CommittableMessage}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer

import scala.util.{Failure, Success, Try}

sealed abstract class KafkaMessageDecoder[F[_, _]: Bitraverse, K, V](
  topicName: KafkaTopicName,
  keyDeserializer: Deserializer[K],
  valueDeserializer: Deserializer[V]
) extends Serializable {

  final def keyDecode(data: Array[Byte]): Try[K] =
    Option(data)
      .traverse(d => Try(keyDeserializer.deserialize(topicName.value, d)))
      .flatMap(
        _.fold[Try[K]](Failure(DecodingNullKeyException(topicName.value)))(
          Success(_)
        )
      )

  final def valueDecode(data: Array[Byte]): Try[V] =
    Option(data)
      .traverse(d => Try(valueDeserializer.deserialize(topicName.value, d)))
      .flatMap(
        _.fold[Try[V]](Failure(DecodingNullValueException(topicName.value)))(
          Success(_)
        )
      )

  final def decodeMessage(
    data: F[Array[Byte], Array[Byte]]
  ): F[Try[K], Try[V]] =
    data.bimap(keyDecode, valueDecode)

  final def decodeKey(
    data: F[Array[Byte], Array[Byte]]
  ): Try[F[K, Array[Byte]]] =
    data.bitraverse(keyDecode, Success(_))

  final def decodeValue(
    data: F[Array[Byte], Array[Byte]]
  ): Try[F[Array[Byte], V]] =
    data.bitraverse(Success(_), valueDecode)

  final def decodeBoth(data: F[Array[Byte], Array[Byte]]): Try[F[K, V]] =
    data.bitraverse(keyDecode, valueDecode)
}

object decoders extends Fs2MessageBitraverse with AkkaMessageBitraverse {

  def consumerRecordDecoder[K: SerdeOf, V: SerdeOf](
    topicName: KafkaTopicName
  ): KafkaMessageDecoder[ConsumerRecord, K, V] =
    new KafkaMessageDecoder[ConsumerRecord, K, V](
      topicName,
      SerdeOf[K].deserializer,
      SerdeOf[V].deserializer
    ) {}

  def fs2MessageDecoder[F[_], K: SerdeOf, V: SerdeOf](
    topicName: KafkaTopicName
  ): KafkaMessageDecoder[Fs2CommittableMessage[F, ?, ?], K, V] =
    new KafkaMessageDecoder[Fs2CommittableMessage[F, ?, ?], K, V](
      topicName,
      SerdeOf[K].deserializer,
      SerdeOf[V].deserializer
    ) {}

  def akkaMessageDecoder[K: SerdeOf, V: SerdeOf](
    topicName: KafkaTopicName
  ): KafkaMessageDecoder[AkkaCommittableMessage, K, V] =
    new KafkaMessageDecoder[AkkaCommittableMessage, K, V](
      topicName,
      SerdeOf[K].deserializer,
      SerdeOf[V].deserializer
    ) {}
}
