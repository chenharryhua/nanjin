package com.github.chenharryhua.nanjin.codec

import cats.implicits._
import fs2.kafka.{KafkaByteProducerRecord, ProducerRecord => Fs2ProducerRecord}
import org.apache.kafka.clients.producer.ProducerRecord

final class KafkaProducerRecordEncoder[K, V](
  topicName: String,
  keyCodec: KafkaCodec.Key[K],
  valueCodec: KafkaCodec.Value[V]
) {

  def record(k: K, v: V): KafkaByteProducerRecord =
    new ProducerRecord(topicName, keyCodec.encode(k), valueCodec.encode(v))

  def record(k: K, v: Array[Byte]): KafkaByteProducerRecord =
    new ProducerRecord(topicName, keyCodec.encode(k), v)

  def record(k: Array[Byte], v: V): KafkaByteProducerRecord =
    new ProducerRecord(topicName, k, valueCodec.encode(v))

  def record(k: Array[Byte], v: Array[Byte]): KafkaByteProducerRecord =
    new ProducerRecord(topicName, k, v)

  def record(pr: ProducerRecord[K, V]): KafkaByteProducerRecord =
    pr.bimap(keyCodec.encode, valueCodec.encode)

  def record(fpr: Fs2ProducerRecord[K, V]): KafkaByteProducerRecord =
    isoFs2ProducerRecord.get(fpr.bimap(keyCodec.encode, valueCodec.encode))
}

final class AkkaMessageEncoder[K, V](topicName: String) {
  import akka.NotUsed
  import akka.kafka.ProducerMessage.Envelope
  import akka.kafka.{ConsumerMessage, ProducerMessage}

  def record(k: K, v: V): ProducerRecord[K, V] =
    new ProducerRecord(topicName, k, v)

  def single(k: K, v: V): Envelope[K, V, NotUsed] =
    ProducerMessage.single(record(k, v))

  def single[P](k: K, v: V, p: P): Envelope[K, V, P] =
    ProducerMessage.single(record(k, v), p)

  def multi(msg: List[(K, V)]): Envelope[K, V, NotUsed] =
    ProducerMessage.multi(msg.map(kv => record(kv._1, kv._2)))

  def multi(
    msg: List[(K, V)],
    cof: ConsumerMessage.CommittableOffset): Envelope[K, V, ConsumerMessage.CommittableOffset] =
    ProducerMessage.multi(msg.map(kv => record(kv._1, kv._2)), cof)
}

final class Fs2MessageEncoder[F[_], K, V](topicName: String) extends Fs2KafkaIso {
  import fs2.Chunk
  import fs2.kafka.{CommittableOffset, ProducerRecords}

  def record(k: K, v: V): Fs2ProducerRecord[K, V] =
    Fs2ProducerRecord(topicName, k, v)

  def single(k: K, v: V): ProducerRecords[K, V, Option[CommittableOffset[F]]] =
    ProducerRecords.one(record(k, v), None)

  def single(
    k: K,
    v: V,
    p: CommittableOffset[F]): ProducerRecords[K, V, Option[CommittableOffset[F]]] =
    ProducerRecords.one(record(k, v), Some(p))

  def multi(msgs: List[(K, V)]): ProducerRecords[K, V, Option[CommittableOffset[F]]] =
    ProducerRecords(msgs.map { case (k, v) => record(k, v) }, None)

  def multi(msgs: Chunk[(K, V, CommittableOffset[F])])
    : ProducerRecords[K, V, Option[CommittableOffset[F]]] =
    ProducerRecords(msgs.map { case (k, v, _) => record(k, v) }, msgs.last.map(_._3))
}
