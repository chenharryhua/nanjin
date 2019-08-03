package com.github.chenharryhua.nanjin.kafka

import akka.kafka.ConsumerMessage.{CommittableMessage => AkkaCommittableMessage}
import cats.Bitraverse
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.CodecException._
import fs2.kafka.{CommittableMessage => Fs2CommittableMessage}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.Deserializer

import scala.util.{Failure, Success, Try}

sealed abstract class KafkaMessageDecoder[G[_, _]: Bitraverse, K, V](
  topicName: String,
  keyDeserializer: Deserializer[K],
  valueDeserializer: Deserializer[V]
) extends Serializable {

  final def keyDecode(data: Array[Byte]): Try[K] =
    Option(data)
      .traverse(d => Try(keyDeserializer.deserialize(topicName, d)))
      .flatMap(
        _.fold[Try[K]](Failure(DecodingNullKeyException(topicName)))(
          Success(_)
        )
      )

  final def valueDecode(data: Array[Byte]): Try[V] =
    Option(data)
      .traverse(d => Try(valueDeserializer.deserialize(topicName, d)))
      .flatMap(
        _.fold[Try[V]](Failure(DecodingNullValueException(topicName)))(
          Success(_)
        )
      )

  final def decodeMessage(
    data: G[Array[Byte], Array[Byte]]
  ): G[Try[K], Try[V]] =
    data.bimap(keyDecode, valueDecode)

  final def decodeKey(
    data: G[Array[Byte], Array[Byte]]
  ): Try[G[K, Array[Byte]]] =
    data.bitraverse(keyDecode, Success(_))

  final def decodeValue(
    data: G[Array[Byte], Array[Byte]]
  ): Try[G[Array[Byte], V]] =
    data.bitraverse(Success(_), valueDecode)

  final def decodeBoth(data: G[Array[Byte], Array[Byte]]): Try[G[K, V]] =
    data.bitraverse(keyDecode, valueDecode)
}

object decoders extends Fs2MessageBitraverse with AkkaMessageBitraverse {

  def consumerRecordDecoder[K, V](
    topicName: String,
    keySerde: KeySerde[K],
    valueSerde: ValueSerde[V]
  ): KafkaMessageDecoder[ConsumerRecord, K, V] =
    new KafkaMessageDecoder[ConsumerRecord, K, V](
      topicName,
      keySerde.deserializer,
      valueSerde.deserializer
    ) {}

  def fs2MessageDecoder[F[_], K, V](
    topicName: String,
    keySerde: KeySerde[K],
    valueSerde: ValueSerde[V]
  ): KafkaMessageDecoder[Fs2CommittableMessage[F, ?, ?], K, V] =
    new KafkaMessageDecoder[Fs2CommittableMessage[F, ?, ?], K, V](
      topicName,
      keySerde.deserializer,
      valueSerde.deserializer
    ) {}

  def akkaMessageDecoder[K, V](
    topicName: String,
    keySerde: KeySerde[K],
    valueSerde: ValueSerde[V]
  ): KafkaMessageDecoder[AkkaCommittableMessage, K, V] =
    new KafkaMessageDecoder[AkkaCommittableMessage, K, V](
      topicName,
      keySerde.deserializer,
      valueSerde.deserializer
    ) {}
}
