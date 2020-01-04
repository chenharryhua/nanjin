package com.github.chenharryhua.nanjin.kafka.api

import cats.data.Chain
import cats.effect.concurrent.Deferred
import cats.effect.{ConcurrentEffect, IO, Sync}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.codec.KafkaCodec
import com.github.chenharryhua.nanjin.kafka.codec.NJProducerMessage._
import com.github.chenharryhua.nanjin.kafka.{KafkaTopicDescription, NJProducerRecord}
import fs2.Chunk
import fs2.kafka.{KafkaByteProducer, KafkaByteProducerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.ByteArraySerializer

sealed trait KafkaProducerApi[F[_], K, V] {
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

  @SuppressWarnings(Array("AsInstanceOf"))
  final def send(v: V): F[RecordMetadata] = send(null.asInstanceOf[K], v)

  def send(pr: NJProducerRecord[K, V]): F[RecordMetadata]
  def send(prs: Chunk[NJProducerRecord[K, V]]): F[Chunk[RecordMetadata]]

  def send(kvs: List[(K, V)]): F[List[RecordMetadata]]
  def send(kvs: Chain[(K, V)]): F[Chain[RecordMetadata]]
}

object KafkaProducerApi {

  def apply[F[_]: ConcurrentEffect, K, V](
    topic: KafkaTopicDescription[K, V]): KafkaProducerApi[F, K, V] =
    new KafkaProducerApiImpl[F, K, V](topic)

  final private[this] class KafkaProducerApiImpl[F[_]: ConcurrentEffect, K, V](
    topic: KafkaTopicDescription[K, V]
  ) extends KafkaProducerApi[F, K, V] {
    private[this] val topicName: String               = topic.topicDef.topicName
    private[this] val keyCodec: KafkaCodec.Key[K]     = topic.codec.keyCodec
    private[this] val valueCodec: KafkaCodec.Value[V] = topic.codec.valueCodec

    private[this] val producer: KafkaByteProducer =
      new KafkaProducer[Array[Byte], Array[Byte]](
        topic.settings.producerSettings.producerProperties,
        new ByteArraySerializer,
        new ByteArraySerializer)

    private def record(k: K, v: V): KafkaByteProducerRecord =
      new ProducerRecord(topicName, keyCodec.encode(k), valueCodec.encode(v))

    private def record(k: K, v: Array[Byte]): KafkaByteProducerRecord =
      new ProducerRecord(topicName, keyCodec.encode(k), v)

    private def record(k: Array[Byte], v: V): KafkaByteProducerRecord =
      new ProducerRecord(topicName, k, valueCodec.encode(v))

    private def record(k: Array[Byte], v: Array[Byte]): KafkaByteProducerRecord =
      new ProducerRecord(topicName, k, v)

    private def record(pr: ProducerRecord[K, V]): KafkaByteProducerRecord =
      pr.bimap(keyCodec.encode, valueCodec.encode)

    private[this] def doSend(data: ProducerRecord[Array[Byte], Array[Byte]]): F[F[RecordMetadata]] =
      Deferred[F, Either[Throwable, RecordMetadata]].flatMap { deferred =>
        Sync[F].delay {
          producer.send(
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
      doSend(record(key, value)).flatten

    override def arbitrarilySend(
      kvs: Chunk[ProducerRecord[Array[Byte], Array[Byte]]]): F[Chunk[RecordMetadata]] =
      kvs.traverse(kv => doSend(kv)).flatMap(_.sequence)

    override def arbitrarilyValueSend(key: K, value: Array[Byte]): F[RecordMetadata] =
      doSend(record(key, value)).flatten

    override def send(pr: NJProducerRecord[K, V]): F[RecordMetadata] =
      doSend(record(pr.toProducerRecord)).flatten

    override def send(prs: Chunk[NJProducerRecord[K, V]]): F[Chunk[RecordMetadata]] =
      prs.traverse(nj => doSend(record(nj.toProducerRecord))).flatMap(_.sequence)

    override def arbitrarilyKeySend(key: Array[Byte], value: V): F[RecordMetadata] =
      doSend(record(key, value)).flatten

    override def send(key: K, value: V): F[RecordMetadata] =
      doSend(record(key, value)).flatten

    override def send(kvs: List[(K, V)]): F[List[RecordMetadata]] =
      kvs.traverse(kv => doSend(record(kv._1, kv._2))).flatMap(_.sequence)

    override def send(kvs: Chain[(K, V)]): F[Chain[RecordMetadata]] =
      kvs.traverse(kv => doSend(record(kv._1, kv._2))).flatMap(_.sequence)
  }
}
