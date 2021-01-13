package com.github.chenharryhua.nanjin.kafka

import akka.kafka.{CommitterSettings => AkkaCommitterSettings, ConsumerSettings => AkkaCS, ProducerSettings => AkkaPS}
import cats.data.Reader
import fs2.kafka.{ConsumerSettings => Fs2CS, ProducerSettings => Fs2PS}

final class AkkaUpdater[K, V](
  val consumer: Reader[AkkaCS[Array[Byte], Array[Byte]], AkkaCS[Array[Byte], Array[Byte]]],
  val producer: Reader[AkkaPS[K, V], AkkaPS[K, V]],
  val committer: Reader[AkkaCommitterSettings, AkkaCommitterSettings]
) extends Serializable {

  def updateConsumer(f: AkkaCS[Array[Byte], Array[Byte]] => AkkaCS[Array[Byte], Array[Byte]]): AkkaUpdater[K, V] =
    new AkkaUpdater[K, V](consumer.andThen(f), producer, committer)

  def updateProducer(f: AkkaPS[K, V] => AkkaPS[K, V]): AkkaUpdater[K, V] =
    new AkkaUpdater[K, V](consumer, producer.andThen(f), committer)

  def updateCommitter(f: AkkaCommitterSettings => AkkaCommitterSettings): AkkaUpdater[K, V] =
    new AkkaUpdater[K, V](consumer, producer, committer.andThen(f))

}

object AkkaUpdater {

  def empty[K, V]: AkkaUpdater[K, V] =
    new AkkaUpdater[K, V](Reader(identity), Reader(identity), Reader(identity))
}

final class Fs2Updater[F[_], K, V](
  val consumer: Reader[Fs2CS[F, Array[Byte], Array[Byte]], Fs2CS[F, Array[Byte], Array[Byte]]],
  val producer: Reader[Fs2PS[F, K, V], Fs2PS[F, K, V]]
) extends Serializable {

  def updateConsumer(f: Fs2CS[F, Array[Byte], Array[Byte]] => Fs2CS[F, Array[Byte], Array[Byte]]): Fs2Updater[F, K, V] =
    new Fs2Updater[F, K, V](consumer.andThen(f), producer)

  def updateProducer(f: Fs2PS[F, K, V] => Fs2PS[F, K, V]): Fs2Updater[F, K, V] =
    new Fs2Updater[F, K, V](consumer, producer.andThen(f))

}

object Fs2Updater {
  def empty[F[_], K, V] = new Fs2Updater[F, K, V](Reader(identity), Reader(identity))
}
