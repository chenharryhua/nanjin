package com.github.chenharryhua.nanjin.kafka

import java.time.{Instant, LocalDate, LocalDateTime}

import cats.implicits._
import cats.kernel.BoundedSemilattice
import monocle.Lens
import monocle.macros.Lenses

import scala.concurrent.duration._

sealed trait ConversionStrategy

object ConversionStrategy {
  case object Intact extends ConversionStrategy
  case object RemovePartition extends ConversionStrategy
  case object RemoveTimestamp extends ConversionStrategy
  case object RemovePartitionAndTimestamp extends ConversionStrategy

  implicit val conversionStrategyLattics: BoundedSemilattice[ConversionStrategy] =
    new BoundedSemilattice[ConversionStrategy] {
      override def empty: ConversionStrategy = Intact

      override def combine(x: ConversionStrategy, y: ConversionStrategy): ConversionStrategy =
        (x, y) match {
          case (Intact, a)                        => a
          case (RemovePartitionAndTimestamp, _)   => RemovePartitionAndTimestamp
          case (_, RemovePartitionAndTimestamp)   => RemovePartitionAndTimestamp
          case (RemovePartition, RemoveTimestamp) => RemovePartitionAndTimestamp
          case (RemoveTimestamp, RemovePartition) => RemovePartitionAndTimestamp
          case (RemovePartition, _)               => RemovePartition
          case (RemoveTimestamp, _)               => RemoveTimestamp
        }
    }
}

@Lenses final case class KafkaUploadRate(batchSize: Int, duration: FiniteDuration)

@Lenses final case class SparkafkaConf(
  timeRange: KafkaDateTimeRange,
  conversionStrategy: ConversionStrategy,
  uploadRate: KafkaUploadRate)

object SparkafkaConf {

  val default: SparkafkaConf =
    SparkafkaConf(
      KafkaDateTimeRange(None, None),
      ConversionStrategy.Intact,
      KafkaUploadRate(1000, 1.seconds))
}

private[kafka] trait SparkafkaModule[F[_], K, V] { self: KafkaTopic[F, K, V] =>

  private def setStartTime(ts: KafkaTimestamp): KafkaTopic[F, K, V] =
    self.copy(
      sparkafkaConf = SparkafkaConf.timeRange
        .composeLens(KafkaDateTimeRange.start)
        .set(Some(ts))(self.sparkafkaConf))

  private def setEndTime(ts: KafkaTimestamp): KafkaTopic[F, K, V] =
    self.copy(sparkafkaConf =
      SparkafkaConf.timeRange.composeLens(KafkaDateTimeRange.end).set(Some(ts))(self.sparkafkaConf))

  def withStartTime(dt: LocalDateTime): KafkaTopic[F, K, V] = setStartTime(KafkaTimestamp(dt))
  def withEndTime(dt: LocalDateTime): KafkaTopic[F, K, V]   = setEndTime(KafkaTimestamp(dt))
  def withStartTime(dt: LocalDate): KafkaTopic[F, K, V]     = setStartTime(KafkaTimestamp(dt))
  def withEndTime(dt: LocalDate): KafkaTopic[F, K, V]       = setEndTime(KafkaTimestamp(dt))
  def withStartTime(dt: Instant): KafkaTopic[F, K, V]       = setStartTime(KafkaTimestamp(dt))
  def withEndTime(dt: Instant): KafkaTopic[F, K, V]         = setEndTime(KafkaTimestamp(dt))

  def withUploadRate(batchSize: Int, duration: FiniteDuration): KafkaTopic[F, K, V] =
    self.copy(
      sparkafkaConf =
        SparkafkaConf.uploadRate.set(KafkaUploadRate(batchSize, duration))(self.sparkafkaConf))

  def withBatchSize(batchSize: Int): KafkaTopic[F, K, V] =
    self.copy(
      sparkafkaConf = SparkafkaConf.uploadRate
        .composeLens(KafkaUploadRate.batchSize)
        .set(batchSize)(self.sparkafkaConf))

  def withDuration(duration: FiniteDuration): KafkaTopic[F, K, V] =
    self.copy(
      sparkafkaConf = SparkafkaConf.uploadRate
        .composeLens(KafkaUploadRate.duration)
        .set(duration)(self.sparkafkaConf))

  private val strategyLens: Lens[SparkafkaConf, ConversionStrategy] =
    SparkafkaConf.conversionStrategy

  def withoutPartition: KafkaTopic[F, K, V] =
    self.copy(
      sparkafkaConf =
        strategyLens.modify(_ |+| ConversionStrategy.RemovePartition)(self.sparkafkaConf))

  def withoutTimestamp: KafkaTopic[F, K, V] =
    self.copy(
      sparkafkaConf =
        strategyLens.modify(_ |+| ConversionStrategy.RemoveTimestamp)(self.sparkafkaConf))

}
