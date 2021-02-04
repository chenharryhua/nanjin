package com.github.chenharryhua.nanjin.kafka

import akka.kafka.{CommitterSettings => AkkaCommitterSettings, ConsumerSettings => AkkaCS, ProducerSettings => AkkaPS}
import cats.data.Reader
import fs2.kafka.{ConsumerSettings => Fs2CS, ProducerSettings => Fs2PS}

final private[kafka] class AkkaSettingsUpdater[K, V](
  val consumer: Reader[AkkaCS[Array[Byte], Array[Byte]], AkkaCS[Array[Byte], Array[Byte]]],
  val producer: Reader[AkkaPS[K, V], AkkaPS[K, V]],
  val committer: Reader[AkkaCommitterSettings, AkkaCommitterSettings]
) extends Serializable {

  def updateConsumer(
    f: AkkaCS[Array[Byte], Array[Byte]] => AkkaCS[Array[Byte], Array[Byte]]): AkkaSettingsUpdater[K, V] =
    new AkkaSettingsUpdater[K, V](consumer.andThen(f), producer, committer)

  def updateProducer(f: AkkaPS[K, V] => AkkaPS[K, V]): AkkaSettingsUpdater[K, V] =
    new AkkaSettingsUpdater[K, V](consumer, producer.andThen(f), committer)

  def updateCommitter(f: AkkaCommitterSettings => AkkaCommitterSettings): AkkaSettingsUpdater[K, V] =
    new AkkaSettingsUpdater[K, V](consumer, producer, committer.andThen(f))

}

private[kafka] object AkkaSettingsUpdater {

  def noUpdate[K, V]: AkkaSettingsUpdater[K, V] =
    new AkkaSettingsUpdater[K, V](Reader(identity), Reader(identity), Reader(identity))
}

final private[kafka] class Fs2SettingsUpdater[F[_], K, V](
  val consumer: Reader[Fs2CS[F, Array[Byte], Array[Byte]], Fs2CS[F, Array[Byte], Array[Byte]]],
  val producer: Reader[Fs2PS[F, K, V], Fs2PS[F, K, V]]
) extends Serializable {

  def updateConsumer(
    f: Fs2CS[F, Array[Byte], Array[Byte]] => Fs2CS[F, Array[Byte], Array[Byte]]): Fs2SettingsUpdater[F, K, V] =
    new Fs2SettingsUpdater[F, K, V](consumer.andThen(f), producer)

  def updateProducer(f: Fs2PS[F, K, V] => Fs2PS[F, K, V]): Fs2SettingsUpdater[F, K, V] =
    new Fs2SettingsUpdater[F, K, V](consumer, producer.andThen(f))

}

private[kafka] object Fs2SettingsUpdater {
  def noUpdate[F[_], K, V] = new Fs2SettingsUpdater[F, K, V](Reader(identity), Reader(identity))
}
