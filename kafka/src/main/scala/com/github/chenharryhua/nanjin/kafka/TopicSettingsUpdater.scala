package com.github.chenharryhua.nanjin.kafka

import cats.Endo
import com.github.chenharryhua.nanjin.common.UpdateConfig

object akkaUpdater {

  import akka.kafka.{CommitterSettings, ConsumerSettings, ProducerSettings}

  final class Consumer(val updates: Endo[ConsumerSettings[Array[Byte], Array[Byte]]])
      extends UpdateConfig[ConsumerSettings[Array[Byte], Array[Byte]], Consumer] {

    override def updateConfig(f: Endo[ConsumerSettings[Array[Byte], Array[Byte]]]): Consumer =
      new Consumer(updates.andThen(f))
  }

  final class Producer[K, V](val updates: Endo[ProducerSettings[K, V]])
      extends UpdateConfig[ProducerSettings[K, V], Producer[K, V]] {

    override def updateConfig(f: Endo[ProducerSettings[K, V]]): Producer[K, V] =
      new Producer[K, V](updates.andThen(f))
  }

  final class Committer(val updates: Endo[CommitterSettings]) extends UpdateConfig[CommitterSettings, Committer] {

    override def updateConfig(f: Endo[CommitterSettings]): Committer =
      new Committer(updates.andThen(f))
  }

  val unitConsumer: Consumer             = new Consumer(identity)
  def unitProducer[K, V]: Producer[K, V] = new Producer[K, V](identity)
  val unitCommitter: Committer           = new Committer(identity)

}

object fs2Updater {
  import fs2.kafka.{ConsumerSettings, ProducerSettings, TransactionalProducerSettings}

  final class Consumer[F[_]](val updates: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]])
      extends UpdateConfig[ConsumerSettings[F, Array[Byte], Array[Byte]], Consumer[F]] {

    override def updateConfig(f: Endo[ConsumerSettings[F, Array[Byte], Array[Byte]]]): Consumer[F] =
      new Consumer(updates.andThen(f))
  }

  final class Producer[F[_], K, V](val updates: Endo[ProducerSettings[F, K, V]])
      extends UpdateConfig[ProducerSettings[F, K, V], Producer[F, K, V]] {

    override def updateConfig(f: Endo[ProducerSettings[F, K, V]]): Producer[F, K, V] =
      new Producer[F, K, V](updates.andThen(f))
  }

  final class TxnProducer[F[_], K, V](val updates: Endo[TransactionalProducerSettings[F, K, V]])
      extends UpdateConfig[TransactionalProducerSettings[F, K, V], TxnProducer[F, K, V]] {
    override def updateConfig(f: Endo[TransactionalProducerSettings[F, K, V]]): TxnProducer[F, K, V] =
      new TxnProducer[F, K, V](updates.andThen(f))
  }

  def unitConsumer[F[_]]: Consumer[F]                   = new Consumer[F](identity)
  def unitProducer[F[_], K, V]: Producer[F, K, V]       = new Producer[F, K, V](identity)
  def unitTxnProducer[F[_], K, V]: TxnProducer[F, K, V] = new TxnProducer[F, K, V](identity)

}
