package com.github.chenharryhua.nanjin.kafka

import cats.Eval
import cats.data.Chain
import cats.effect.concurrent.Deferred
import cats.effect.{ConcurrentEffect, IO, Sync}
import cats.implicits._
import fs2.Chunk
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

trait KafkaProducerApi[F[_], K, V] {
  def arbitrarilySend(key: Array[Byte], value: Array[Byte]): F[RecordMetadata]
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

  def send(rec: ConsumerRecord[K, V]): F[RecordMetadata]

  def send(kvs: List[(K, V)]): F[List[RecordMetadata]]
  def send(kvs: Chain[(K, V)]): F[Chain[RecordMetadata]]
  def send(kvs: Chunk[(K, V)]): F[Chunk[RecordMetadata]]

}

object KafkaProducerApi {
  type RawProducer = KafkaProducer[Array[Byte], Array[Byte]]

  def apply[F[_]: ConcurrentEffect, K, V](
    producer: Eval[RawProducer],
    encoder: encoders.ProducerRecordEncoder[K, V]): KafkaProducerApi[F, K, V] =
    new KafkaProducerApiImpl[F, K, V](producer, encoder)

  final private[this] class KafkaProducerApiImpl[F[_]: ConcurrentEffect, K, V](
    producer: Eval[RawProducer],
    encoder: encoders.ProducerRecordEncoder[K, V]
  ) extends KafkaProducerApi[F, K, V] {

    private[this] def doSend(data: ProducerRecord[Array[Byte], Array[Byte]]): F[F[RecordMetadata]] =
      Deferred[F, Either[Throwable, RecordMetadata]].flatMap { deferred =>
        Sync[F].delay {
          producer.value.send(
            data,
            (metadata: RecordMetadata, exception: Exception) => {
              val complete = deferred.complete {
                Option(exception).fold[Either[Throwable, RecordMetadata]](Right(metadata))(ex =>
                  Left(ex))
              }
              ConcurrentEffect[F].runAsync(complete)(_ => IO.unit).unsafeRunSync()
            }
          )
        }.as(deferred.get.rethrow)
      }

    override def arbitrarilySend(key: Array[Byte], value: Array[Byte]): F[RecordMetadata] =
      doSend(encoder.record(key, value)).flatten

    override def arbitrarilyValueSend(key: K, value: Array[Byte]): F[RecordMetadata] =
      doSend(encoder.record(key, value)).flatten

    override def arbitrarilyKeySend(key: Array[Byte], value: V): F[RecordMetadata] =
      doSend(encoder.record(key, value)).flatten

    override def send(key: K, value: V): F[RecordMetadata] =
      doSend(encoder.record(key, value)).flatten

    override def send(kvs: List[(K, V)]): F[List[RecordMetadata]] =
      kvs.traverse(kv => doSend(encoder.record(kv._1, kv._2))).flatMap(_.sequence)

    override def send(kvs: Chain[(K, V)]): F[Chain[RecordMetadata]] =
      kvs.traverse(kv => doSend(encoder.record(kv._1, kv._2))).flatMap(_.sequence)

    override def send(kvs: Chunk[(K, V)]): F[Chunk[RecordMetadata]] =
      kvs.traverse(kv => doSend(encoder.record(kv._1, kv._2))).flatMap(_.sequence)

    override def send(rec: ConsumerRecord[K, V]): F[RecordMetadata] =
      doSend(encoder.cloneRecord(rec)).flatten
  }
}
