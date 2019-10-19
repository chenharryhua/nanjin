package com.github.chenharryhua.nanjin.sparkafka

import java.time._

import cats.implicits._
import cats.kernel.BoundedSemilattice
import com.github.chenharryhua.nanjin.kafka.{KafkaDateTimeRange, KafkaTimestamp, KafkaTopic}
import monocle.Lens
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka010.{LocationStrategies, LocationStrategy}

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

object KafkaUploadRate {
  val default: KafkaUploadRate = KafkaUploadRate(1000, 1.second)
}

final case class StorageRootPath(value: String) extends AnyVal {

  def path[F[_]](topic: KafkaTopic[F, _, _]): String = value + topic.topicDef.topicName
}

object StorageRootPath {
  val default: StorageRootPath = StorageRootPath("./data/kafka/parquet/")
}

@Lenses final case class SparKafkaParams private (
  timeRange: KafkaDateTimeRange,
  conversionStrategy: ConversionStrategy,
  uploadRate: KafkaUploadRate,
  zoneId: ZoneId,
  rootPath: StorageRootPath,
  saveMode: SaveMode,
  locationStrategy: LocationStrategy) {

  def withZoneId(zoneId: ZoneId): SparKafkaParams     = copy(zoneId   = zoneId)
  def withStorageRootPath(p: String): SparKafkaParams = copy(rootPath = StorageRootPath(p))
  def withSaveMode(sm: SaveMode): SparKafkaParams     = copy(saveMode = sm)
  def withOverwrite: SparKafkaParams                  = copy(saveMode = SaveMode.Overwrite)

  def withLocationStrategy(ls: LocationStrategy): SparKafkaParams = copy(locationStrategy = ls)

  private def setStartTime(ts: KafkaTimestamp): SparKafkaParams =
    SparKafkaParams.timeRange.composeLens(KafkaDateTimeRange.start).set(Some(ts))(this)

  private def setEndTime(ts: KafkaTimestamp): SparKafkaParams =
    SparKafkaParams.timeRange.composeLens(KafkaDateTimeRange.end).set(Some(ts))(this)

  def withStartTime(dt: Instant): SparKafkaParams       = setStartTime(KafkaTimestamp(dt))
  def withEndTime(dt: Instant): SparKafkaParams         = setEndTime(KafkaTimestamp(dt))
  def withStartTime(dt: ZonedDateTime): SparKafkaParams = setStartTime(KafkaTimestamp(dt))
  def withEndTime(dt: ZonedDateTime): SparKafkaParams   = setEndTime(KafkaTimestamp(dt))
  def withStartTime(dt: LocalDateTime): SparKafkaParams = setStartTime(KafkaTimestamp(dt, zoneId))
  def withEndTime(dt: LocalDateTime): SparKafkaParams   = setEndTime(KafkaTimestamp(dt, zoneId))
  def withStartTime(dt: LocalDate): SparKafkaParams     = setStartTime(KafkaTimestamp(dt, zoneId))
  def withEndTime(dt: LocalDate): SparKafkaParams       = setEndTime(KafkaTimestamp(dt, zoneId))

  def withinOneDay(dt: LocalDate): SparKafkaParams =
    withStartTime(dt).withEndTime(dt.plusDays(1))

  def withToday: SparKafkaParams     = withinOneDay(LocalDate.now)
  def withYesterday: SparKafkaParams = withinOneDay(LocalDate.now.minusDays(1))

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
      KafkaDateTimeRange.infinite,
      ConversionStrategy.Intact,
      KafkaUploadRate.default,
      ZoneId.systemDefault(),
      StorageRootPath.default,
      SaveMode.ErrorIfExists,
      LocationStrategies.PreferConsistent
    )
}
