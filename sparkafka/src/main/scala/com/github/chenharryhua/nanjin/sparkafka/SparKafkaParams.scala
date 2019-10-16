package com.github.chenharryhua.nanjin.sparkafka

import java.time.Instant

import cats.implicits._
import cats.kernel.BoundedSemilattice
import com.github.chenharryhua.nanjin.kafka.{KafkaDateTimeRange, KafkaTimestamp}
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

@Lenses final case class SparKafkaParams(
  timeRange: KafkaDateTimeRange,
  conversionStrategy: ConversionStrategy,
  uploadRate: KafkaUploadRate) {

  private def setStartTime(ts: KafkaTimestamp): SparKafkaParams =
    SparKafkaParams.timeRange.composeLens(KafkaDateTimeRange.start).set(Some(ts))(this)

  private def setEndTime(ts: KafkaTimestamp): SparKafkaParams =
    SparKafkaParams.timeRange.composeLens(KafkaDateTimeRange.end).set(Some(ts))(this)
  def withStartTime(dt: Instant): SparKafkaParams = setStartTime(KafkaTimestamp(dt))
  def withEndTime(dt: Instant): SparKafkaParams   = setEndTime(KafkaTimestamp(dt))

  def withBatchSize(batchSize: Int): SparKafkaParams =
    SparKafkaParams.uploadRate.composeLens(KafkaUploadRate.batchSize).set(batchSize)(this)

  def withDuration(duration: FiniteDuration): SparKafkaParams =
    SparKafkaParams.uploadRate.composeLens(KafkaUploadRate.duration).set(duration)(this)

  def withUploadRate(batchSize: Int, duration: FiniteDuration): SparKafkaParams =
    withBatchSize(batchSize).withDuration(duration)

  private val strategyLens: Lens[SparKafkaParams, ConversionStrategy] =
    SparKafkaParams.conversionStrategy

  def withoutPartition: SparKafkaParams =
    strategyLens.modify(_ |+| ConversionStrategy.RemovePartition)(this)

  def withoutTimestamp: SparKafkaParams =
    strategyLens.modify(_ |+| ConversionStrategy.RemoveTimestamp)(this)

}

object SparKafkaParams {

  val default: SparKafkaParams =
    SparKafkaParams(
      KafkaDateTimeRange(None, None),
      ConversionStrategy.Intact,
      KafkaUploadRate(1000, 1.seconds))
}
