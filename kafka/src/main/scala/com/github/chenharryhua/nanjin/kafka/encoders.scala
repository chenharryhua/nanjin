package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serializer

import scala.collection.immutable

object encoders extends Fs2MessageBitraverse with AkkaMessageBitraverse {

  def producerRecordEncoder[K, V](
    topicName: KafkaTopicName,
    keySerde: KeySerde[K],
    valueSerde: ValueSerde[V]): ProducerRecordEncoder[K, V] =
    new ProducerRecordEncoder(topicName, keySerde.serializer, valueSerde.serializer)

  def akkaMessageEncoder[K, V](topicName: KafkaTopicName): AkkaMessageEncoder[K, V] =
    new AkkaMessageEncoder[K, V](topicName)

  def fs2MessageEncoder[F[_], K, V](topicName: KafkaTopicName): Fs2MessageEncoder[F, K, V] =
    new Fs2MessageEncoder[F, K, V](topicName)

  final class ProducerRecordEncoder[K, V](
    topicName: KafkaTopicName,
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V])
      extends Serializable {

    private val tName: String = topicName.value

    def cloneRecord(rec: ConsumerRecord[K, V]): ProducerRecord[Array[Byte], Array[Byte]] =
      recordTopicLens
        .set(tName)(rec)
        .bimap(keySerializer.serialize(tName, _), valueSerializer.serialize(tName, _))

    def record(k: Array[Byte], v: Array[Byte]): ProducerRecord[Array[Byte], Array[Byte]] =
      new ProducerRecord(topicName.value, k, v)

    def record(k: K, v: Array[Byte]): ProducerRecord[Array[Byte], Array[Byte]] =
      new ProducerRecord(topicName.value, keySerializer.serialize(tName, k), v)

    def record(k: Array[Byte], v: V): ProducerRecord[Array[Byte], Array[Byte]] =
      new ProducerRecord(tName, k, valueSerializer.serialize(tName, v))

    def record(k: K, v: V): ProducerRecord[Array[Byte], Array[Byte]] =
      new ProducerRecord(
        tName,
        keySerializer.serialize(tName, k),
        valueSerializer.serialize(tName, v))
  }

  final class AkkaMessageEncoder[K, V](topicName: KafkaTopicName) extends Serializable {
    import akka.NotUsed
    import akka.kafka.ConsumerMessage.CommittableMessage
    import akka.kafka.ProducerMessage.Envelope
    import akka.kafka.{ConsumerMessage, ProducerMessage}

    def record(k: K, v: V): ProducerRecord[K, V]    = new ProducerRecord(topicName.value, k, v)
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
      recordTopicLens.set(topicName.value)(cm.record)

    def cloneSingle(
      cm: CommittableMessage[K, V]): Envelope[K, V, ConsumerMessage.CommittableOffset] =
      ProducerMessage.single(cloneRecord(cm), cm.committableOffset)

    import cats.data.NonEmptyList

    def cloneMulti(
      cms: NonEmptyList[CommittableMessage[K, V]]
    ): Envelope[K, V, ConsumerMessage.CommittableOffset] =
      ProducerMessage.multi(cms.map(cloneRecord).toList, cms.last.committableOffset)
  }

  final class Fs2MessageEncoder[F[_], K, V](topicName: KafkaTopicName) extends Serializable {
    import fs2.Chunk
    import fs2.kafka.{CommittableMessage, CommittableOffset, Id, ProducerMessage, ProducerRecord}

    def record(k: K, v: V): ProducerRecord[K, V] = ProducerRecord(topicName.value, k, v)

    def single(k: K, v: V): ProducerMessage[Id, K, V, Option[CommittableOffset[F]]] =
      ProducerMessage.one(record(k, v), None)

    def single(
      k: K,
      v: V,
      p: CommittableOffset[F]): ProducerMessage[Id, K, V, Option[CommittableOffset[F]]] =
      ProducerMessage.one(record(k, v), Some(p))

    def multi(msgs: Chunk[(K, V, CommittableOffset[F])])
      : ProducerMessage[Chunk, K, V, Option[CommittableOffset[F]]] =
      ProducerMessage(msgs.map { case (k, v, _) => record(k, v) }, msgs.last.map(_._3))

    def multi(msgs: List[(K, V)]): ProducerMessage[List, K, V, Option[CommittableOffset[F]]] =
      ProducerMessage(msgs.map { case (k, v) => record(k, v) }, None)

    def cloneRecord(cm: CommittableMessage[F, K, V]): ProducerRecord[K, V] = {
      fs2ProducerRecordIso.reverseGet(recordTopicLens.set(topicName.value)(cm.record))
    }

    def cloneSingle(
      cm: CommittableMessage[F, K, V]): ProducerMessage[Id, K, V, Option[CommittableOffset[F]]] =
      ProducerMessage.one(cloneRecord(cm), Some(cm.committableOffset))

    def cloneMulti(
      cms: Chunk[CommittableMessage[F, K, V]]
    ): ProducerMessage[Chunk, K, V, Option[CommittableOffset[F]]] =
      ProducerMessage(cms.map(cloneRecord), cms.last.map(_.committableOffset))
  }
}
