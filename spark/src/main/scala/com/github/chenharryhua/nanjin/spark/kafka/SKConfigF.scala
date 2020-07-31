package com.github.chenharryhua.nanjin.spark.kafka

import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.concurrent.TimeUnit

import cats.derived.auto.functor._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.TopicName
import com.github.chenharryhua.nanjin.spark.NJShowDataset
import higherkindness.droste.data.Fix
import higherkindness.droste.macros.deriveFixedPoint
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka010.{LocationStrategies, LocationStrategy}

import scala.concurrent.duration._

@Lenses final private[spark] case class NJUploadRate(batchSize: Int, duration: FiniteDuration)

private[spark] object NJUploadRate {
  val default: NJUploadRate = NJUploadRate(batchSize = 1000, duration = 1.second)
}

@Lenses final private[spark] case class SKParams private (
  timeRange: NJDateTimeRange,
  uploadRate: NJUploadRate,
  replayPath: TopicName => String,
  saveMode: SaveMode,
  locationStrategy: LocationStrategy,
  showDs: NJShowDataset)

private[spark] object SKParams {

  def apply(zoneId: ZoneId): SKParams =
    SKParams(
      timeRange = NJDateTimeRange(zoneId),
      uploadRate = NJUploadRate.default,
      replayPath = topicName => s"./data/sparKafka/${topicName.value}/replay/",
      saveMode = SaveMode.ErrorIfExists,
      locationStrategy = LocationStrategies.PreferConsistent,
      showDs = NJShowDataset(20, isTruncate = false)
    )
}

@deriveFixedPoint sealed private[spark] trait SKConfigF[_]

private[spark] object SKConfigF {
  final case class DefaultParams[K](zoneId: ZoneId) extends SKConfigF[K]

  final case class WithBatchSize[K](value: Int, cont: K) extends SKConfigF[K]
  final case class WithDuration[K](value: FiniteDuration, cont: K) extends SKConfigF[K]

  final case class WithStartTimeStr[K](value: String, cont: K) extends SKConfigF[K]
  final case class WithStartTime[K](value: LocalDateTime, cont: K) extends SKConfigF[K]
  final case class WithEndTimeStr[K](value: String, cont: K) extends SKConfigF[K]
  final case class WithEndTime[K](value: LocalDateTime, cont: K) extends SKConfigF[K]
  final case class WithZoneId[K](value: ZoneId, cont: K) extends SKConfigF[K]
  final case class WithTimeRange[K](value: NJDateTimeRange, cont: K) extends SKConfigF[K]
  final case class WithNSeconds[K](value: Long, cont: K) extends SKConfigF[K]
  final case class WithOneDay[K](value: LocalDate, cont: K) extends SKConfigF[K]
  final case class WithOneDayStr[K](value: String, cont: K) extends SKConfigF[K]

  final case class WithSaveMode[K](value: SaveMode, cont: K) extends SKConfigF[K]

  final case class WithLocationStrategy[K](value: LocationStrategy, cont: K) extends SKConfigF[K]

  final case class WithShowRows[K](value: Int, cont: K) extends SKConfigF[K]
  final case class WithShowTruncate[K](isTruncate: Boolean, cont: K) extends SKConfigF[K]

  final case class WithReplayPath[K](value: TopicName => String, cont: K) extends SKConfigF[K]

  private val algebra: Algebra[SKConfigF, SKParams] = Algebra[SKConfigF, SKParams] {
    case DefaultParams(v)           => SKParams(v)
    case WithBatchSize(v, c)        => SKParams.uploadRate.composeLens(NJUploadRate.batchSize).set(v)(c)
    case WithDuration(v, c)         => SKParams.uploadRate.composeLens(NJUploadRate.duration).set(v)(c)
    case WithStartTimeStr(v, c)     => SKParams.timeRange.modify(_.withStartTime(v))(c)
    case WithEndTimeStr(v, c)       => SKParams.timeRange.modify(_.withEndTime(v))(c)
    case WithStartTime(v, c)        => SKParams.timeRange.modify(_.withStartTime(v))(c)
    case WithEndTime(v, c)          => SKParams.timeRange.modify(_.withEndTime(v))(c)
    case WithZoneId(v, c)           => SKParams.timeRange.modify(_.withZoneId(v))(c)
    case WithTimeRange(v, c)        => SKParams.timeRange.set(v)(c)
    case WithNSeconds(v, c)         => SKParams.timeRange.modify(_.withNSeconds(v))(c)
    case WithOneDay(v, c)           => SKParams.timeRange.modify(_.withOneDay(v))(c)
    case WithOneDayStr(v, c)        => SKParams.timeRange.modify(_.withOneDay(v))(c)
    case WithSaveMode(v, c)         => SKParams.saveMode.set(v)(c)
    case WithLocationStrategy(v, c) => SKParams.locationStrategy.set(v)(c)
    case WithShowRows(v, c)         => SKParams.showDs.composeLens(NJShowDataset.rowNum).set(v)(c)
    case WithShowTruncate(v, c)     => SKParams.showDs.composeLens(NJShowDataset.isTruncate).set(v)(c)
    case WithReplayPath(v, c)       => SKParams.replayPath.set(v)(c)
  }

  def evalConfig(cfg: SKConfig): SKParams = scheme.cata(algebra).apply(cfg.value)
}

final private[spark] case class SKConfig private (value: Fix[SKConfigF]) extends AnyVal {
  import SKConfigF._

  def withBatchSize(bs: Int): SKConfig           = SKConfig(Fix(WithBatchSize(bs, value)))
  def withDuration(fd: FiniteDuration): SKConfig = SKConfig(Fix(WithDuration(fd, value)))
  def withDuration(ms: Long): SKConfig           = withDuration(FiniteDuration(ms, TimeUnit.MILLISECONDS))

  def withStartTime(s: String): SKConfig                  = SKConfig(Fix(WithStartTimeStr(s, value)))
  def withStartTime(s: LocalDateTime): SKConfig           = SKConfig(Fix(WithStartTime(s, value)))
  def withEndTime(s: String): SKConfig                    = SKConfig(Fix(WithEndTimeStr(s, value)))
  def withEndTime(s: LocalDateTime): SKConfig             = SKConfig(Fix(WithEndTime(s, value)))
  def withZoneId(s: ZoneId): SKConfig                     = SKConfig(Fix(WithZoneId(s, value)))
  def withTimeRange(tr: NJDateTimeRange): SKConfig        = SKConfig(Fix(WithTimeRange(tr, value)))
  def withTimeRange(start: String, end: String): SKConfig = withStartTime(start).withEndTime(end)
  def withNSeconds(s: Long): SKConfig                     = SKConfig(Fix(WithNSeconds(s, value)))
  def withOneDay(s: String): SKConfig                     = SKConfig(Fix(WithOneDayStr(s, value)))
  def withOneDay(s: LocalDate): SKConfig                  = SKConfig(Fix(WithOneDay(s, value)))
  def withToday: SKConfig                                 = withOneDay(LocalDate.now)
  def withYesterday: SKConfig                             = withOneDay(LocalDate.now.minusDays(1))

  def withLocationStrategy(ls: LocationStrategy): SKConfig =
    SKConfig(Fix(WithLocationStrategy(ls, value)))

  def withShowRows(num: Int): SKConfig = SKConfig(Fix(WithShowRows(num, value)))
  def withoutTruncate: SKConfig        = SKConfig(Fix(WithShowTruncate(isTruncate = false, value)))
  def withTruncate: SKConfig           = SKConfig(Fix(WithShowTruncate(isTruncate = true, value)))

  def withSaveMode(sm: SaveMode): SKConfig = SKConfig(Fix(WithSaveMode(sm, value)))
  def withOverwrite: SKConfig              = withSaveMode(SaveMode.Overwrite)

  def withReplayPath(f: TopicName => String): SKConfig =
    SKConfig(Fix(WithReplayPath(f, value)))

  def evalConfig: SKParams = SKConfigF.evalConfig(this)
}

private[spark] object SKConfig {

  def apply(zoneId: ZoneId): SKConfig =
    SKConfig(Fix(SKConfigF.DefaultParams[Fix[SKConfigF]](zoneId)))

  def apply(dtr: NJDateTimeRange): SKConfig =
    apply(dtr.zoneId).withTimeRange(dtr)
}
