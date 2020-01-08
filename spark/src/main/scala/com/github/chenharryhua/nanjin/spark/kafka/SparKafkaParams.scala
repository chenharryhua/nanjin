package com.github.chenharryhua.nanjin.spark.kafka

import java.time._

import cats.data.Reader
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.TopicName
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka010.{LocationStrategies, LocationStrategy}

import scala.concurrent.duration._

@Lenses final case class NJRate(batchSize: Int, duration: FiniteDuration)

object NJRate {
  val default: NJRate = NJRate(1000, 1.second)
}

@Lenses final case class ConversionTactics(keepPartition: Boolean, keepTimestamp: Boolean)

object ConversionTactics {

  def default: ConversionTactics =
    ConversionTactics(keepPartition = false, keepTimestamp = true)
}

@Lenses final case class SparKafkaParams private (
  timeRange: NJDateTimeRange,
  conversionTactics: ConversionTactics,
  uploadRate: NJRate,
  zoneId: ZoneId,
  pathBuilder: Reader[TopicName, String],
  saveMode: SaveMode,
  locationStrategy: LocationStrategy,
  repartition: Int) {

  val clock: Clock = Clock.system(zoneId)

  def withZoneId(zoneId: ZoneId): SparKafkaParams = copy(zoneId   = zoneId)
  def withSaveMode(sm: SaveMode): SparKafkaParams = copy(saveMode = sm)
  def withOverwrite: SparKafkaParams              = copy(saveMode = SaveMode.Overwrite)

  def withPathBuilder(rp: TopicName => String): SparKafkaParams = copy(pathBuilder = Reader(rp))

  def withLocationStrategy(ls: LocationStrategy): SparKafkaParams = copy(locationStrategy = ls)

  private def setStartTime(ts: NJTimestamp): SparKafkaParams =
    SparKafkaParams.timeRange.composeLens(NJDateTimeRange.start).set(Some(ts))(this)

  private def setEndTime(ts: NJTimestamp): SparKafkaParams =
    SparKafkaParams.timeRange.composeLens(NJDateTimeRange.end).set(Some(ts))(this)

  def withStartTime(dt: Instant): SparKafkaParams       = setStartTime(NJTimestamp(dt))
  def withEndTime(dt: Instant): SparKafkaParams         = setEndTime(NJTimestamp(dt))
  def withStartTime(dt: ZonedDateTime): SparKafkaParams = setStartTime(NJTimestamp(dt))
  def withEndTime(dt: ZonedDateTime): SparKafkaParams   = setEndTime(NJTimestamp(dt))
  def withStartTime(dt: LocalDateTime): SparKafkaParams = setStartTime(NJTimestamp(dt, zoneId))
  def withEndTime(dt: LocalDateTime): SparKafkaParams   = setEndTime(NJTimestamp(dt, zoneId))
  def withStartTime(dt: LocalDate): SparKafkaParams     = setStartTime(NJTimestamp(dt, zoneId))
  def withEndTime(dt: LocalDate): SparKafkaParams       = setEndTime(NJTimestamp(dt, zoneId))

  def withinOneDay(dt: LocalDate): SparKafkaParams =
    withStartTime(dt).withEndTime(dt.plusDays(1))

  def withToday: SparKafkaParams     = withinOneDay(LocalDate.now)
  def withYesterday: SparKafkaParams = withinOneDay(LocalDate.now.minusDays(1))

  def withBatchSize(batchSize: Int): SparKafkaParams =
    SparKafkaParams.uploadRate.composeLens(NJRate.batchSize).set(batchSize)(this)

  def withDuration(duration: FiniteDuration): SparKafkaParams =
    SparKafkaParams.uploadRate.composeLens(NJRate.duration).set(duration)(this)

  def withUploadRate(batchSize: Int, duration: FiniteDuration): SparKafkaParams =
    withBatchSize(batchSize).withDuration(duration)

  def withoutPartition: SparKafkaParams =
    SparKafkaParams.conversionTactics.composeLens(ConversionTactics.keepPartition).set(false)(this)

  def withPartition: SparKafkaParams =
    SparKafkaParams.conversionTactics.composeLens(ConversionTactics.keepPartition).set(true)(this)

  def withoutTimestamp: SparKafkaParams =
    SparKafkaParams.conversionTactics.composeLens(ConversionTactics.keepTimestamp).set(false)(this)

  def withTimestamp: SparKafkaParams =
    SparKafkaParams.conversionTactics.composeLens(ConversionTactics.keepTimestamp).set(true)(this)

  def withSparkRepartition(number: Int): SparKafkaParams =
    SparKafkaParams.repartition.set(number)(this)
}

object SparKafkaParams {

  val default: SparKafkaParams =
    SparKafkaParams(
      NJDateTimeRange.infinite,
      ConversionTactics.default,
      NJRate.default,
      ZoneId.systemDefault(),
      Reader(tn => s"./data/kafka/parquet/$tn"),
      SaveMode.ErrorIfExists,
      LocationStrategies.PreferConsistent,
      30
    )
}
