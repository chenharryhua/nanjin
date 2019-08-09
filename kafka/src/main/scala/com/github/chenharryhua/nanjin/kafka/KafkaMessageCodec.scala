package com.github.chenharryhua.nanjin.kafka

import akka.kafka.ConsumerMessage.CommittableMessage
import cats.Bitraverse
import cats.implicits._
import fs2.kafka.{CommittableConsumerRecord, KafkaByteConsumerRecord, KafkaByteProducerRecord}
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import scala.util.{Success, Try}

sealed abstract class KafkaMessageDecode[F[_, _]: Bitraverse, K, V](
  keyIso: Iso[Array[Byte], K],
  valueIso: Iso[Array[Byte], V]) {

  final def decode(data: F[Array[Byte], Array[Byte]]): F[K, V] =
    data.bimap(keyIso.get, valueIso.get)

  final def decodeKey(data: F[Array[Byte], Array[Byte]]): F[K, Array[Byte]] =
    data.bimap(keyIso.get, identity)

  final def decodeValue(data: F[Array[Byte], Array[Byte]]): F[Array[Byte], V] =
    data.bimap(identity, valueIso.get)

  final def safeDecodeKeyValue(data: F[Array[Byte], Array[Byte]]): F[Try[K], Try[V]] =
    data.bimap(
      k => utils.nullable(k).flatMap(x => Try(keyIso.get(x))),
      v => utils.nullable(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecode(data: F[Array[Byte], Array[Byte]]): Try[F[K, V]] =
    data.bitraverse(
      k => utils.nullable(k).flatMap(x => Try(keyIso.get(x))),
      v => utils.nullable(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecodeValue(data: F[Array[Byte], Array[Byte]]): Try[F[Array[Byte], V]] =
    data.bitraverse(Success(_), v => utils.nullable(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecodeKey(data: F[Array[Byte], Array[Byte]]): Try[F[K, Array[Byte]]] =
    data.bitraverse(k => utils.nullable(k).flatMap(x => Try(keyIso.get(x))), Success(_))
}

sealed trait KafkaConsumerRecordDecode[K, V] extends KafkaRecordBitraverse {
  def keyIso: Iso[Array[Byte], K]
  def valueIso: Iso[Array[Byte], V]

  final def decode(cr: KafkaByteConsumerRecord): ConsumerRecord[K, V] =
    cr.bimap(keyIso.get, valueIso.get)

  final def decodeKey(cr: KafkaByteConsumerRecord): ConsumerRecord[K, Array[Byte]] =
    cr.bimap(keyIso.get, identity)

  final def decodeValue(cr: KafkaByteConsumerRecord): ConsumerRecord[Array[Byte], V] =
    cr.bimap(identity, valueIso.get)

  final def safeDecodeKeyValue(data: KafkaByteConsumerRecord): ConsumerRecord[Try[K], Try[V]] =
    data.bimap(
      k => utils.nullable(k).flatMap(x => Try(keyIso.get(x))),
      v => utils.nullable(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecode(data: KafkaByteConsumerRecord): Try[ConsumerRecord[K, V]] =
    data.bitraverse(
      k => utils.nullable(k).flatMap(x => Try(keyIso.get(x))),
      v => utils.nullable(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecodeValue(data: KafkaByteConsumerRecord): Try[ConsumerRecord[Array[Byte], V]] =
    data.bitraverse(Success(_), v => utils.nullable(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecodeKey(data: KafkaByteConsumerRecord): Try[ConsumerRecord[K, Array[Byte]]] =
    data.bitraverse(k => utils.nullable(k).flatMap(x => Try(keyIso.get(x))), Success(_))
}

sealed trait KafkaConsumerRecordEncode[K, V] {
  def keyIso: Iso[Array[Byte], K]
  def valueIso: Iso[Array[Byte], V]
  def topicName: String

  final def record(k: K, v: V): KafkaByteProducerRecord =
    new ProducerRecord(topicName, keyIso.reverseGet(k), valueIso.reverseGet(v))
  final def record(k: K, v: Array[Byte]): KafkaByteProducerRecord =
    new ProducerRecord(topicName, keyIso.reverseGet(k), v)
  final def record(k: Array[Byte], v: V): KafkaByteProducerRecord =
    new ProducerRecord(topicName, k, valueIso.reverseGet(v))
  final def record(k: Array[Byte], v: Array[Byte]): KafkaByteProducerRecord =
    new ProducerRecord(topicName, k, v)
}

sealed trait AkkaMessageEncode[K, V] {
  import akka.NotUsed
  import akka.kafka.ProducerMessage.Envelope
  import akka.kafka.{ConsumerMessage, ProducerMessage}

  def topicName: String
  final private def record(k: K, v: V): ProducerRecord[K, V] = new ProducerRecord(topicName, k, v)

  final def single(k: K, v: V): Envelope[K, V, NotUsed] = ProducerMessage.single(record(k, v))

  final def single[P](k: K, v: V, p: P): Envelope[K, V, P] =
    ProducerMessage.single(record(k, v), p)

  final def multi(msg: List[(K, V)]): Envelope[K, V, NotUsed] =
    ProducerMessage.multi(msg.map(kv => record(kv._1, kv._2)))

  final def multi(
    msg: List[(K, V)],
    cof: ConsumerMessage.CommittableOffset): Envelope[K, V, ConsumerMessage.CommittableOffset] =
    ProducerMessage.multi(msg.map(kv => record(kv._1, kv._2)), cof)
}

sealed trait Fs2MessageEncode[F[_], K, V] {
  import fs2.Chunk
  import fs2.kafka.{CommittableOffset, Id, ProducerRecords, ProducerRecord => Fs2ProducerRecord}
  def topicName: String

  final private def record(k: K, v: V): Fs2ProducerRecord[K, V] = Fs2ProducerRecord(topicName, k, v)

  final def single(k: K, v: V): ProducerRecords[Id, K, V, Option[CommittableOffset[F]]] =
    ProducerRecords.one(record(k, v), None)

  final def single(
    k: K,
    v: V,
    p: CommittableOffset[F]): ProducerRecords[Id, K, V, Option[CommittableOffset[F]]] =
    ProducerRecords.one(record(k, v), Some(p))

  final def multi(msgs: List[(K, V)]): ProducerRecords[List, K, V, Option[CommittableOffset[F]]] =
    ProducerRecords(msgs.map { case (k, v) => record(k, v) }, None)

  final def multi(msgs: Chunk[(K, V, CommittableOffset[F])])
    : ProducerRecords[Chunk, K, V, Option[CommittableOffset[F]]] =
    ProducerRecords(msgs.map { case (k, v, _) => record(k, v) }, msgs.last.map(_._3))
}

object codec extends AkkaMessageBitraverse with Fs2MessageBitraverse {

  trait ConsumerRecordCodec[K, V]
      extends KafkaConsumerRecordDecode[K, V] with KafkaConsumerRecordEncode[K, V]

  abstract class Fs2Codec[F[_], K, V](keyIso: Iso[Array[Byte], K], valueIso: Iso[Array[Byte], V])
      extends KafkaMessageDecode[CommittableConsumerRecord[F, ?, ?], K, V](keyIso, valueIso)
      with Fs2MessageEncode[F, K, V] with ConsumerRecordCodec[K, V]

  abstract class AkkaCodec[K, V](keyIso: Iso[Array[Byte], K], valueIso: Iso[Array[Byte], V])
      extends KafkaMessageDecode[CommittableMessage, K, V](keyIso, valueIso)
      with AkkaMessageEncode[K, V] with ConsumerRecordCodec[K, V]

}
