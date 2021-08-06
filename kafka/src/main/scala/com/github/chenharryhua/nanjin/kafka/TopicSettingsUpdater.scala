package com.github.chenharryhua.nanjin.kafka

import cats.data.Reader
import com.github.chenharryhua.nanjin.common.UpdateConfig

object akkaUpdater {

  import akka.kafka.{CommitterSettings, ConsumerSettings, ProducerSettings}

  final class Consumer(
    val updates: Reader[ConsumerSettings[Array[Byte], Array[Byte]], ConsumerSettings[Array[Byte], Array[Byte]]])
      extends UpdateConfig[ConsumerSettings[Array[Byte], Array[Byte]], Consumer] {

    override def updateConfig(
      f: ConsumerSettings[Array[Byte], Array[Byte]] => ConsumerSettings[Array[Byte], Array[Byte]]): Consumer =
      new Consumer(updates.andThen(f))
  }

  final class Producer[K, V](val updates: Reader[ProducerSettings[K, V], ProducerSettings[K, V]])
      extends UpdateConfig[ProducerSettings[K, V], Producer[K, V]] {

    override def updateConfig(f: ProducerSettings[K, V] => ProducerSettings[K, V]): Producer[K, V] =
      new Producer[K, V](updates.andThen(f))
  }

  final class Committer(val updates: Reader[CommitterSettings, CommitterSettings])
      extends UpdateConfig[CommitterSettings, Committer] {

    override def updateConfig(f: CommitterSettings => CommitterSettings): Committer =
      new Committer(updates.andThen(f))
  }

  val unitConsumer: Consumer             = new Consumer(Reader(identity))
  def unitProducer[K, V]: Producer[K, V] = new Producer[K, V](Reader(identity))
  val unitCommitter: Committer           = new Committer(Reader(identity))

}

object fs2Updater {
  import fs2.kafka.{ConsumerSettings, ProducerSettings, TransactionalProducerSettings}

  final class Consumer[F[_]](
    val updates: Reader[ConsumerSettings[F, Array[Byte], Array[Byte]], ConsumerSettings[F, Array[Byte], Array[Byte]]])
      extends UpdateConfig[ConsumerSettings[F, Array[Byte], Array[Byte]], Consumer[F]] {

    override def updateConfig(
      f: ConsumerSettings[F, Array[Byte], Array[Byte]] => ConsumerSettings[F, Array[Byte], Array[Byte]]): Consumer[F] =
      new Consumer(updates.andThen(f))
  }

  final class Producer[F[_], K, V](val updates: Reader[ProducerSettings[F, K, V], ProducerSettings[F, K, V]])
      extends UpdateConfig[ProducerSettings[F, K, V], Producer[F, K, V]] {

    override def updateConfig(f: ProducerSettings[F, K, V] => ProducerSettings[F, K, V]): Producer[F, K, V] =
      new Producer[F, K, V](updates.andThen(f))
  }

  final class TxnProducer[F[_], K, V](
    val updates: Reader[TransactionalProducerSettings[F, K, V], TransactionalProducerSettings[F, K, V]])
      extends UpdateConfig[TransactionalProducerSettings[F, K, V], TxnProducer[F, K, V]] {
    override def updateConfig(
      f: TransactionalProducerSettings[F, K, V] => TransactionalProducerSettings[F, K, V]): TxnProducer[F, K, V] =
      new TxnProducer[F, K, V](updates.andThen(f))
  }

  def unitConsumer[F[_]]: Consumer[F]                   = new Consumer[F](Reader(identity))
  def unitProducer[F[_], K, V]: Producer[F, K, V]       = new Producer[F, K, V](Reader(identity))
  def unitTxnProducer[F[_], K, V]: TxnProducer[F, K, V] = new TxnProducer[F, K, V](Reader(identity))

}
