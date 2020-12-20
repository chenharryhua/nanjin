package com.github.chenharryhua.nanjin.kafka

import akka.NotUsed
import akka.kafka.ProducerMessage
import cats.Traverse
import cats.effect.{ConcurrentEffect, ContextShift, Resource}
import cats.syntax.all._
import fs2.kafka.{
  producerResource,
  KafkaProducer => Fs2KafkaProducer,
  ProducerRecord => Fs2ProducerRecord,
  ProducerRecords => Fs2ProducerRecords,
  ProducerResult => Fs2ProducerResult
}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.immutable

private[kafka] trait KafkaTopicProducer[F[_], K, V] {
  topic: KafkaTopic[F, K, V] =>

  private def fs2ProducerResource(implicit
    concurrentEffect: ConcurrentEffect[F],
    contextShift: ContextShift[F]): Resource[F, Fs2KafkaProducer[F, K, V]] =
    producerResource[F].using(topic.fs2ProducerSettings)

  def send(k: K, v: V)(implicit
    concurrentEffect: ConcurrentEffect[F],
    contextShift: ContextShift[F]): F[Fs2ProducerResult[K, V, Unit]] =
    fs2ProducerResource.use(_.produce(Fs2ProducerRecords.one(fs2PR(k, v)))).flatten

  def send[G[+_]: Traverse](list: G[Fs2ProducerRecord[K, V]])(implicit
    concurrentEffect: ConcurrentEffect[F],
    contextShift: ContextShift[F]): F[Fs2ProducerResult[K, V, Unit]] =
    fs2ProducerResource.use(_.produce(Fs2ProducerRecords(list))).flatten

  def send(pr: Fs2ProducerRecord[K, V])(implicit
    concurrentEffect: ConcurrentEffect[F],
    contextShift: ContextShift[F]): F[Fs2ProducerResult[K, V, Unit]] =
    fs2ProducerResource.use(_.produce(Fs2ProducerRecords.one(pr))).flatten

  def fs2PR(k: K, v: V): Fs2ProducerRecord[K, V] = Fs2ProducerRecord(topicName.value, k, v)

  def akkaProducerRecord[P](key: K, value: V, p: P): ProducerMessage.Envelope[K, V, P] =
    ProducerMessage.single[K, V, P](
      new ProducerRecord[K, V](topicDef.topicName.value, key, value),
      p)

  def akkaProducerRecord(key: K, value: V): ProducerMessage.Envelope[K, V, NotUsed] =
    akkaProducerRecord[NotUsed](key, value, NotUsed)

  def akkaProducerRecords[P](seq: immutable.Seq[(K, V)], p: P): ProducerMessage.Envelope[K, V, P] =
    ProducerMessage.multi(
      seq.map { case (k, v) => new ProducerRecord(topicDef.topicName.value, k, v) },
      p)

  def akkaProducerRecords(seq: immutable.Seq[(K, V)]): ProducerMessage.Envelope[K, V, NotUsed] =
    akkaProducerRecords[NotUsed](seq, NotUsed)
}
