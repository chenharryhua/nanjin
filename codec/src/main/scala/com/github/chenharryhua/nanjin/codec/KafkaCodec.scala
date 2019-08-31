package com.github.chenharryhua.nanjin.codec

import cats.Bitraverse
import cats.implicits._
import fs2.kafka.{KafkaByteConsumerRecord, KafkaByteProducerRecord}
import monocle.Iso
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

import scala.util.{Success, Try}

abstract class KafkaMessageDecode[F[_, _]: Bitraverse, K, V](
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
      k => utils.checkNull(k).flatMap(x => Try(keyIso.get(x))),
      v => utils.checkNull(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecode(data: F[Array[Byte], Array[Byte]]): Try[F[K, V]] =
    data.bitraverse(
      k => utils.checkNull(k).flatMap(x => Try(keyIso.get(x))),
      v => utils.checkNull(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecodeValue(data: F[Array[Byte], Array[Byte]]): Try[F[Array[Byte], V]] =
    data.bitraverse(Success(_), v => utils.checkNull(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecodeKey(data: F[Array[Byte], Array[Byte]]): Try[F[K, Array[Byte]]] =
    data.bitraverse(k => utils.checkNull(k).flatMap(x => Try(keyIso.get(x))), Success(_))
}

trait KafkaConsumerRecordDecode[K, V] extends BitraverseKafkaRecord {
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
      k => utils.checkNull(k).flatMap(x => Try(keyIso.get(x))),
      v => utils.checkNull(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecode(data: KafkaByteConsumerRecord): Try[ConsumerRecord[K, V]] =
    data.bitraverse(
      k => utils.checkNull(k).flatMap(x => Try(keyIso.get(x))),
      v => utils.checkNull(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecodeValue(data: KafkaByteConsumerRecord): Try[ConsumerRecord[Array[Byte], V]] =
    data.bitraverse(Success(_), v => utils.checkNull(v).flatMap(x => Try(valueIso.get(x))))

  final def safeDecodeKey(data: KafkaByteConsumerRecord): Try[ConsumerRecord[K, Array[Byte]]] =
    data.bitraverse(k => utils.checkNull(k).flatMap(x => Try(keyIso.get(x))), Success(_))
}

trait KafkaProducerRecordEncode[K, V] {
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
  final def record(nj: NJProducerRecord[K, V]): KafkaByteProducerRecord =
    nj.bimap(keyIso.reverseGet, valueIso.reverseGet).producerRecord
}

trait AkkaMessageEncode[K, V] {
  import akka.NotUsed
  import akka.kafka.ProducerMessage.Envelope
  import akka.kafka.{ConsumerMessage, ProducerMessage}

  def topicName: String
  final private def record(k: K, v: V): ProducerRecord[K, V] = new ProducerRecord(topicName, k, v)

  final def single(k: K, v: V): Envelope[K, V, NotUsed] = ProducerMessage.single(record(k, v))

  final def single[P](k: K, v: V, p: P): Envelope[K, V, P] =
    ProducerMessage.single(record(k, v), p)

  final def single(nj: NJProducerRecord[K, V]): Envelope[K, V, NotUsed] =
    ProducerMessage.single(nj.producerRecord)
  final def single[P](nj: NJProducerRecord[K, V], p: P): Envelope[K, V, P] =
    ProducerMessage.single(nj.producerRecord, p)

  final def multi(msg: List[(K, V)]): Envelope[K, V, NotUsed] =
    ProducerMessage.multi(msg.map(kv => record(kv._1, kv._2)))

  final def multi(
    msg: List[(K, V)],
    cof: ConsumerMessage.CommittableOffset): Envelope[K, V, ConsumerMessage.CommittableOffset] =
    ProducerMessage.multi(msg.map(kv => record(kv._1, kv._2)), cof)
}

trait Fs2MessageEncode[F[_], K, V] extends BitraverseFs2Message {
  import fs2.Chunk
  import fs2.kafka.{CommittableOffset, ProducerRecords, ProducerRecord => Fs2ProducerRecord}
  def topicName: String

  final private def record(k: K, v: V): Fs2ProducerRecord[K, V] = Fs2ProducerRecord(topicName, k, v)

  final def single(k: K, v: V): ProducerRecords[K, V, Option[CommittableOffset[F]]] =
    ProducerRecords.one(record(k, v), None)

  final def single(
    k: K,
    v: V,
    p: CommittableOffset[F]): ProducerRecords[K, V, Option[CommittableOffset[F]]] =
    ProducerRecords.one(record(k, v), Some(p))

  final def single(
    nj: NJProducerRecord[K, V]): ProducerRecords[K, V, Option[CommittableOffset[F]]] =
    ProducerRecords.one(nj.fs2ProducerRecord, None)

  final def single(
    nj: NJProducerRecord[K, V],
    p: CommittableOffset[F]): ProducerRecords[K, V, Option[CommittableOffset[F]]] =
    ProducerRecords.one(nj.fs2ProducerRecord, Some(p))

  final def multi(msgs: List[(K, V)]): ProducerRecords[K, V, Option[CommittableOffset[F]]] =
    ProducerRecords(msgs.map { case (k, v) => record(k, v) }, None)

  final def multi(msgs: Chunk[(K, V, CommittableOffset[F])])
    : ProducerRecords[K, V, Option[CommittableOffset[F]]] =
    ProducerRecords(msgs.map { case (k, v, _) => record(k, v) }, msgs.last.map(_._3))
}
