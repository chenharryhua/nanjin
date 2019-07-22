package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Serde, Serializer}

import scala.collection.immutable

trait KafkaMessageEncoder extends Serializable {
  protected def topicName: KafkaTopicName
}

object encoders {

  final class ProducerRecordEncoder[K, V](
    val topicName: KafkaTopicName,
    keySerde: Serde[K],
    valueSerde: Serde[V])
      extends KafkaMessageEncoder with KafkaMessageBifunctor {

    private val keySer: Serializer[K]   = keySerde.serializer()
    private val valueSer: Serializer[V] = valueSerde.serializer()
    private val tName: String           = topicName.value

    def cloneRecord(rec: ConsumerRecord[K, V]): ProducerRecord[Array[Byte], Array[Byte]] =
      super
        .fromConsumerRecord(tName, rec)
        .bimap(keySer.serialize(tName, _), valueSer.serialize(tName, _))

    def record(k: Array[Byte], v: Array[Byte]): ProducerRecord[Array[Byte], Array[Byte]] =
      new ProducerRecord(topicName.value, k, v)

    def record(k: K, v: Array[Byte]): ProducerRecord[Array[Byte], Array[Byte]] =
      new ProducerRecord(topicName.value, keySer.serialize(tName, k), v)

    def record(k: Array[Byte], v: V): ProducerRecord[Array[Byte], Array[Byte]] =
      new ProducerRecord(tName, k, valueSer.serialize(tName, v))

    def record(k: K, v: V): ProducerRecord[Array[Byte], Array[Byte]] =
      new ProducerRecord(tName, keySer.serialize(tName, k), valueSer.serialize(tName, v))
  }

  trait AkkaMessageEncoder[K, V] extends KafkaMessageEncoder with KafkaMessageBifunctor {
    import akka.NotUsed
    import akka.kafka.ConsumerMessage.CommittableMessage
    import akka.kafka.ProducerMessage.Envelope
    import akka.kafka.{ConsumerMessage, ProducerMessage}

    final def record(k: K, v: V): ProducerRecord[K, V]    = new ProducerRecord(topicName.value, k, v)
    final def single(k: K, v: V): Envelope[K, V, NotUsed] = ProducerMessage.single(record(k, v))
    final def single[P](k: K, v: V, p: P): Envelope[K, V, P] =
      ProducerMessage.single(record(k, v), p)

    final def multi(msg: immutable.Seq[(K, V)]): Envelope[K, V, NotUsed] =
      ProducerMessage.multi(msg.map(kv => record(kv._1, kv._2)))

    final def multi(
      msg: immutable.Seq[(K, V)],
      cof: ConsumerMessage.CommittableOffset): Envelope[K, V, ConsumerMessage.CommittableOffset] =
      ProducerMessage.multi(msg.map(kv => record(kv._1, kv._2)), cof)

    final def cloneRecord(cm: CommittableMessage[K, V]): ProducerRecord[K, V] =
      super.fromConsumerRecord(topicName.value, cm.record)

    final def cloneSingle(
      cm: CommittableMessage[K, V]): Envelope[K, V, ConsumerMessage.CommittableOffset] =
      ProducerMessage.single(cloneRecord(cm), cm.committableOffset)

    import cats.data.NonEmptyList
    final def cloneMulti(
      cms: NonEmptyList[CommittableMessage[K, V]]
    ): Envelope[K, V, ConsumerMessage.CommittableOffset] =
      ProducerMessage.multi(cms.map(cloneRecord).toList, cms.last.committableOffset)

  }

  trait Fs2MessageEncoder[F[_], K, V] extends KafkaMessageEncoder with Fs2MessageBifunctor {
    import fs2.Chunk
    import fs2.kafka.{CommittableMessage, CommittableOffset, Id, ProducerMessage, ProducerRecord}

    final def record(k: K, v: V): ProducerRecord[K, V] = ProducerRecord(topicName.value, k, v)

    final def single(k: K, v: V): ProducerMessage[Id, K, V, Option[CommittableOffset[F]]] =
      ProducerMessage.one(record(k, v), None)

    final def single(
      k: K,
      v: V,
      p: CommittableOffset[F]): ProducerMessage[Id, K, V, Option[CommittableOffset[F]]] =
      ProducerMessage.one(record(k, v), Some(p))

    final def multi(msgs: Chunk[(K, V, CommittableOffset[F])])
      : ProducerMessage[Chunk, K, V, Option[CommittableOffset[F]]] =
      ProducerMessage(msgs.map { case (k, v, _) => record(k, v) }, msgs.last.map(_._3))

    final def multi(msgs: List[(K, V)]): ProducerMessage[List, K, V, Option[CommittableOffset[F]]] =
      ProducerMessage(msgs.map { case (k, v) => record(k, v) }, None)

    final def cloneRecord(cm: CommittableMessage[F, K, V]): ProducerRecord[K, V] = {
      super.fs2ProducerRecordIso.reverseGet(super.fromConsumerRecord(topicName.value, cm.record))
    }

    final def cloneSingle(
      cm: CommittableMessage[F, K, V]): ProducerMessage[Id, K, V, Option[CommittableOffset[F]]] =
      ProducerMessage.one(cloneRecord(cm), Some(cm.committableOffset))

    final def cloneMulti(
      cms: Chunk[CommittableMessage[F, K, V]]
    ): ProducerMessage[Chunk, K, V, Option[CommittableOffset[F]]] =
      ProducerMessage(cms.map(cloneRecord), cms.last.map(_.committableOffset))
  }
}
