package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serializer

import scala.collection.immutable

object encoders extends Fs2MessageBitraverse with AkkaMessageBitraverse {

  def producerRecordEncoder[K, V](
    topicName: String,
    keySerde: KeySerde[K],
    valueSerde: ValueSerde[V]): ProducerRecordEncoder[K, V] =
    new ProducerRecordEncoder(topicName, keySerde.serializer, valueSerde.serializer)

  def akkaMessageEncoder[K, V](topicName: String): AkkaMessageEncoder[K, V] =
    new AkkaMessageEncoder[K, V](topicName)

  def fs2MessageEncoder[F[_], K, V](topicName: String): Fs2MessageEncoder[F, K, V] =
    new Fs2MessageEncoder[F, K, V](topicName)

  final class ProducerRecordEncoder[K, V](
    topicName: String,
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V])
      extends Serializable {

    def cloneRecord(rec: ConsumerRecord[K, V]): ProducerRecord[Array[Byte], Array[Byte]] =
      recordTopicLens
        .set(topicName)(rec)
        .bimap(keySerializer.serialize(topicName, _), valueSerializer.serialize(topicName, _))

    def record(k: Array[Byte], v: Array[Byte]): ProducerRecord[Array[Byte], Array[Byte]] =
      new ProducerRecord(topicName, k, v)

    def record(k: K, v: Array[Byte]): ProducerRecord[Array[Byte], Array[Byte]] =
      new ProducerRecord(topicName, keySerializer.serialize(topicName, k), v)

    def record(k: Array[Byte], v: V): ProducerRecord[Array[Byte], Array[Byte]] =
      new ProducerRecord(topicName, k, valueSerializer.serialize(topicName, v))

    def record(k: K, v: V): ProducerRecord[Array[Byte], Array[Byte]] =
      new ProducerRecord(
        topicName,
        keySerializer.serialize(topicName, k),
        valueSerializer.serialize(topicName, v))
  }

  final class AkkaMessageEncoder[K, V](topicName: String) extends Serializable {
    import akka.NotUsed
    import akka.kafka.ConsumerMessage.CommittableMessage
    import akka.kafka.ProducerMessage.Envelope
    import akka.kafka.{ConsumerMessage, ProducerMessage}

    def record(k: K, v: V): ProducerRecord[K, V]    = new ProducerRecord(topicName, k, v)
    def single(k: K, v: V): Envelope[K, V, NotUsed] = ProducerMessage.single(record(k, v))

    def single[P](k: K, v: V, p: P): Envelope[K, V, P] =
      ProducerMessage.single(record(k, v), p)

    def multi(msg: immutable.Seq[(K, V)]): Envelope[K, V, NotUsed] =
      ProducerMessage.multi(msg.map(kv => record(kv._1, kv._2)))

    def multi(
      msg: immutable.Seq[(K, V)],
      cof: ConsumerMessage.CommittableOffset): Envelope[K, V, ConsumerMessage.CommittableOffset] =
      ProducerMessage.multi(msg.map(kv => record(kv._1, kv._2)), cof)

    def cloneRecord(cm: CommittableMessage[K, V]): ProducerRecord[K, V] =
      recordTopicLens.set(topicName)(cm.record)

    def cloneSingle(
      cm: CommittableMessage[K, V]): Envelope[K, V, ConsumerMessage.CommittableOffset] =
      ProducerMessage.single(cloneRecord(cm), cm.committableOffset)

    import cats.data.NonEmptyList

    def cloneMulti(
      cms: NonEmptyList[CommittableMessage[K, V]]
    ): Envelope[K, V, ConsumerMessage.CommittableOffset] =
      ProducerMessage.multi(cms.map(cloneRecord).toList, cms.last.committableOffset)
  }

  final class Fs2MessageEncoder[F[_], K, V](topicName: String) extends Serializable {
    import fs2.Chunk
    import fs2.kafka.{CommittableConsumerRecord, CommittableOffset, Id, ProducerRecords, ProducerRecord}

    def record(k: K, v: V): ProducerRecord[K, V] = ProducerRecord(topicName, k, v)

    def single(k: K, v: V): ProducerRecords[Id, K, V, Option[CommittableOffset[F]]] =
    ProducerRecords.one(record(k, v), None)

    def single(
      k: K,
      v: V,
      p: CommittableOffset[F]): ProducerRecords[Id, K, V, Option[CommittableOffset[F]]] =
      ProducerRecords.one(record(k, v), Some(p))

    def multi(msgs: Chunk[(K, V, CommittableOffset[F])])
      : ProducerRecords[Chunk, K, V, Option[CommittableOffset[F]]] =
      ProducerRecords(msgs.map { case (k, v, _) => record(k, v) }, msgs.last.map(_._3))

    def multi(msgs: List[(K, V)]): ProducerRecords[List, K, V, Option[CommittableOffset[F]]] =
    ProducerRecords(msgs.map { case (k, v) => record(k, v) }, None)

    def cloneRecord(cm: CommittableConsumerRecord[F, K, V]): ProducerRecord[K, V] = {
      fs2ProducerRecordIso.reverseGet(recordTopicLens.set(topicName)(fs2ComsumerRecordIso.get(cm.record)))
    }

    def cloneSingle(
      cm: CommittableConsumerRecord[F, K, V]): ProducerRecords[Id, K, V, Option[CommittableOffset[F]]] =
      ProducerRecords.one(cloneRecord(cm), Some(cm.offset))

    def cloneMulti(
      cms: Chunk[CommittableConsumerRecord[F, K, V]]
    ): ProducerRecords[Chunk, K, V, Option[CommittableOffset[F]]] =
    ProducerRecords(cms.map(cloneRecord), cms.last.map(_.offset))
  }
}
