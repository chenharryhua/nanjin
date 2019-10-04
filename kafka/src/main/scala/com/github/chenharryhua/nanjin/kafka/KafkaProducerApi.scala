package com.github.chenharryhua.nanjin.kafka

import cats.Eval
import cats.data.Chain
import cats.effect.concurrent.Deferred
import cats.effect.{ConcurrentEffect, IO, Sync}
import cats.implicits._
import cats.tagless._
import com.github.chenharryhua.nanjin.codec._
import fs2.Chunk
import fs2.kafka.{KafkaByteProducer, ProducerRecord => Fs2ProducerRecord}
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}

@autoFunctorK
@autoSemigroupalK
trait KafkaProducerApi[F[_], K, V] {
  def arbitrarilySend(key: Array[Byte], value: Array[Byte]): F[RecordMetadata]

  def arbitrarilySend(
    kvs: Chunk[ProducerRecord[Array[Byte], Array[Byte]]]): F[Chunk[RecordMetadata]]

  final def arbitrarilySend(kv: (Array[Byte], Array[Byte])): F[RecordMetadata] =
    arbitrarilySend(kv._1, kv._2)

  def arbitrarilyValueSend(key: K, value: Array[Byte]): F[RecordMetadata]

  final def arbitrarilyValueSend(kv: (K, Array[Byte])): F[RecordMetadata] =
    arbitrarilyValueSend(kv._1, kv._2)

  def arbitrarilyKeySend(key: Array[Byte], value: V): F[RecordMetadata]

  final def arbitrarilyKeySend(kv: (Array[Byte], V)): F[RecordMetadata] =
    arbitrarilyKeySend(kv._1, kv._2)

  def send(key: K, value: V): F[RecordMetadata]
  final def send(kv: (K, V)): F[RecordMetadata] = send(kv._1, kv._2)
  final def send(v: V): F[RecordMetadata]       = send(null.asInstanceOf[K], v)

  def send(pr: ProducerRecord[K, V]): F[RecordMetadata]
  def send(prs: Chunk[ProducerRecord[K, V]]): F[Chunk[RecordMetadata]]

  def send(fpr: Fs2ProducerRecord[K, V]): F[RecordMetadata]

  def send(kvs: List[(K, V)]): F[List[RecordMetadata]]
  def send(kvs: Chain[(K, V)]): F[Chain[RecordMetadata]]
}

object KafkaProducerApi {

  def apply[F[_]: ConcurrentEffect, K, V](topic: KafkaTopic[F, K, V]): KafkaProducerApi[F, K, V] =
    new KafkaProducerApiImpl[F, K, V](topic)

  final private[this] class KafkaProducerApiImpl[F[_]: ConcurrentEffect, K, V](
    topic: KafkaTopic[F, K, V]
  ) extends KafkaProducerApi[F, K, V] {
    private[this] val topicName: String                 = topic.topicName
    private[this] val keyCodec: KafkaCodec[K]           = topic.keyCodec
    private[this] val valueCodec: KafkaCodec[V]         = topic.valueCodec
    private[this] val producer: Eval[KafkaByteProducer] = topic.sharedProducer

    private[this] val encoder: KafkaProducerRecordEncoder[K, V] =
      new KafkaProducerRecordEncoder[K, V](topicName, keyCodec, valueCodec)

    private[this] def doSend(data: ProducerRecord[Array[Byte], Array[Byte]]): F[F[RecordMetadata]] =
      Deferred[F, Either[Throwable, RecordMetadata]].flatMap { deferred =>
        Sync[F].delay {
          producer.value.send(
            data,
            (metadata: RecordMetadata, exception: Exception) => {
              val complete = deferred.complete {
                Option(exception).fold[Either[Throwable, RecordMetadata]](Right(metadata))(Left(_))
              }
              ConcurrentEffect[F].runAsync(complete)(_ => IO.unit).unsafeRunSync()
            }
          )
        }.as(deferred.get.rethrow)
      }

    override def arbitrarilySend(key: Array[Byte], value: Array[Byte]): F[RecordMetadata] =
      doSend(encoder.record(key, value)).flatten

    override def arbitrarilySend(
      kvs: Chunk[ProducerRecord[Array[Byte], Array[Byte]]]): F[Chunk[RecordMetadata]] =
      kvs.traverse(kv => doSend(kv)).flatMap(_.sequence)

    override def arbitrarilyValueSend(key: K, value: Array[Byte]): F[RecordMetadata] =
      doSend(encoder.record(key, value)).flatten

    override def send(pr: ProducerRecord[K, V]): F[RecordMetadata] =
      doSend(encoder.record(pr)).flatten

    override def send(fpr: Fs2ProducerRecord[K, V]): F[RecordMetadata] =
      doSend(encoder.record(fpr)).flatten

    override def arbitrarilyKeySend(key: Array[Byte], value: V): F[RecordMetadata] =
      doSend(encoder.record(key, value)).flatten

    override def send(key: K, value: V): F[RecordMetadata] =
      doSend(encoder.record(key, value)).flatten

    override def send(kvs: List[(K, V)]): F[List[RecordMetadata]] =
      kvs.traverse(kv => doSend(encoder.record(kv._1, kv._2))).flatMap(_.sequence)

    override def send(kvs: Chain[(K, V)]): F[Chain[RecordMetadata]] =
      kvs.traverse(kv => doSend(encoder.record(kv._1, kv._2))).flatMap(_.sequence)

    override def send(prs: Chunk[ProducerRecord[K, V]]): F[Chunk[RecordMetadata]] =
      prs.traverse(pr => doSend(encoder.record(pr))).flatMap(_.sequence)
  }
}
