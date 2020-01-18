package com.github.chenharryhua.nanjin.spark.kafka

import java.time._

import cats.data.Reader
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.TopicName
import com.github.chenharryhua.nanjin.spark.NJFileFormat
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka010.{LocationStrategies, LocationStrategy}
import shapeless.{:+:, CNil, Coproduct, Poly1}

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
  startTime: Option[SparKafkaParams.TimeParam],
  endTime: Option[SparKafkaParams.TimeParam],
  conversionTactics: ConversionTactics,
  uploadRate: NJRate,
  zoneId: ZoneId,
  pathBuilder: Reader[TopicName, String],
  fileFormat: NJFileFormat,
  saveMode: SaveMode,
  locationStrategy: LocationStrategy,
  repartition: Int) {

  private object calcDate extends Poly1 {
    implicit val localDate      = at[LocalDate](NJTimestamp(_, zoneId))
    implicit val localDateTime  = at[LocalDateTime](NJTimestamp(_, zoneId))
    implicit val instant        = at[Instant](NJTimestamp(_))
    implicit val zonedDateTime  = at[ZonedDateTime](NJTimestamp(_))
    implicit val offsetDateTime = at[OffsetDateTime](NJTimestamp(_))
    implicit val longTime       = at[Long](NJTimestamp(_))
  }

  def timeRange: NJDateTimeRange =
    NJDateTimeRange(startTime.map(_.fold(calcDate)), endTime.map(_.fold(calcDate)))

  val clock: Clock = Clock.system(zoneId)

  def withZoneId(zoneId: ZoneId): SparKafkaParams = copy(zoneId   = zoneId)
  def withSaveMode(sm: SaveMode): SparKafkaParams = copy(saveMode = sm)
  def withOverwrite: SparKafkaParams              = copy(saveMode = SaveMode.Overwrite)

  def withPathBuilder(rp: TopicName => String): SparKafkaParams = copy(pathBuilder = Reader(rp))

  def withFileFormat(ff: NJFileFormat): SparKafkaParams = copy(fileFormat = ff)
  def withJson: SparKafkaParams                         = withFileFormat(NJFileFormat.Json)
  def withAvro: SparKafkaParams                         = withFileFormat(NJFileFormat.Avro)
  def withParquet: SparKafkaParams                      = withFileFormat(NJFileFormat.Parquet)

  def withLocationStrategy(ls: LocationStrategy): SparKafkaParams = copy(locationStrategy = ls)

//start-time
  def withStartTime(dt: LocalDate): SparKafkaParams =
    SparKafkaParams.startTime.set(Some(Coproduct[SparKafkaParams.TimeParam](dt)))(this)

  def withStartTime(dt: LocalDateTime): SparKafkaParams =
    SparKafkaParams.startTime.set(Some(Coproduct[SparKafkaParams.TimeParam](dt)))(this)

  def withStartTime(dt: Instant): SparKafkaParams =
    SparKafkaParams.startTime.set(Some(Coproduct[SparKafkaParams.TimeParam](dt)))(this)

  def withStartTime(dt: ZonedDateTime): SparKafkaParams =
    SparKafkaParams.startTime.set(Some(Coproduct[SparKafkaParams.TimeParam](dt)))(this)

  def withStartTime(dt: OffsetDateTime): SparKafkaParams =
    SparKafkaParams.startTime.set(Some(Coproduct[SparKafkaParams.TimeParam](dt)))(this)

  def withStartTime(dt: Long): SparKafkaParams =
    SparKafkaParams.startTime.set(Some(Coproduct[SparKafkaParams.TimeParam](dt)))(this)

//end-time
  def withEndTime(dt: LocalDate): SparKafkaParams =
    SparKafkaParams.endTime.set(Some(Coproduct[SparKafkaParams.TimeParam](dt)))(this)

  def withEndTime(dt: LocalDateTime): SparKafkaParams =
    SparKafkaParams.endTime.set(Some(Coproduct[SparKafkaParams.TimeParam](dt)))(this)

  def withEndTime(dt: Instant): SparKafkaParams =
    SparKafkaParams.endTime.set(Some(Coproduct[SparKafkaParams.TimeParam](dt)))(this)

  def withEndTime(dt: ZonedDateTime): SparKafkaParams =
    SparKafkaParams.endTime.set(Some(Coproduct[SparKafkaParams.TimeParam](dt)))(this)

  def withEndTime(dt: OffsetDateTime): SparKafkaParams =
    SparKafkaParams.endTime.set(Some(Coproduct[SparKafkaParams.TimeParam](dt)))(this)

  def withEndTime(dt: Long): SparKafkaParams =
    SparKafkaParams.endTime.set(Some(Coproduct[SparKafkaParams.TimeParam](dt)))(this)

// daily
  def withinOneDay(dt: LocalDate): SparKafkaParams =
    withStartTime(dt).withEndTime(dt.plusDays(1))

  def withToday: SparKafkaParams     = withinOneDay(LocalDate.now)
  def withYesterday: SparKafkaParams = withinOneDay(LocalDate.now.minusDays(1))

// others
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

  final type TimeParam =
    LocalDate :+: LocalDateTime :+: Instant :+: ZonedDateTime :+: OffsetDateTime :+: Long :+: CNil

  val default: SparKafkaParams =
    SparKafkaParams(
      None,
      None,
      ConversionTactics.default,
      NJRate.default,
      ZoneId.systemDefault(),
      Reader(tn => s"./data/spark/kafka/$tn"),
      NJFileFormat.Parquet,
      SaveMode.ErrorIfExists,
      LocationStrategies.PreferConsistent,
      30
    )
}
