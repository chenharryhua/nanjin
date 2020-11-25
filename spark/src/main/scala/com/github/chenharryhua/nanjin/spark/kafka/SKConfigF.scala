package com.github.chenharryhua.nanjin.spark.kafka

import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.concurrent.TimeUnit

import cats.derived.auto.functor._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.TopicName
import com.github.chenharryhua.nanjin.spark.NJShowDataset
import higherkindness.droste.data.Fix
import higherkindness.droste.macros.deriveFixedPoint
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka010.{LocationStrategies, LocationStrategy}

import scala.concurrent.duration._

@Lenses final private[kafka] case class NJUploadParams(
  batchSize: Int,
  duration: FiniteDuration,
  recordsLimit: Long,
  timeLimit: FiniteDuration)

private[kafka] object NJUploadParams {

  val default: NJUploadParams = NJUploadParams(
    batchSize = 1000,
    duration = 1.second,
    Long.MaxValue,
    FiniteDuration(Long.MaxValue, TimeUnit.NANOSECONDS))
}

@Lenses final private[kafka] case class SKParams private (
  topicName: TopicName,
  timeRange: NJDateTimeRange,
  uploadParams: NJUploadParams,
  replayPathBuilder: TopicName => String,
  pathBuilder: (TopicName, NJFileFormat) => String,
  datePartitionPathBuilder: (TopicName, NJFileFormat, LocalDate) => String,
  saveMode: SaveMode,
  locationStrategy: LocationStrategy,
  showDs: NJShowDataset) {
  def outPath(fmt: NJFileFormat): String = pathBuilder(topicName, fmt)
  val replayPath: String                 = replayPathBuilder(topicName)

  def datePartition(fmt: NJFileFormat): LocalDate => String =
    datePartitionPathBuilder(topicName, fmt, _)
}

private[kafka] object SKParams {

  def apply(topicName: TopicName, zoneId: ZoneId): SKParams = {
    val partitionPath: (TopicName, NJFileFormat, LocalDate) => String =
      (tn, fmt, date) =>
        s"./data/sparKafka/${tn.value}/${NJTimestamp(date, zoneId).`Year=yyyy/Month=mm/Day=dd`(
          zoneId)}/${fmt.alias}.${fmt.format}"
    SKParams(
      topicName = topicName,
      timeRange = NJDateTimeRange(zoneId),
      uploadParams = NJUploadParams.default,
      replayPathBuilder = topicName => s"./data/sparKafka/${topicName.value}/replay/",
      pathBuilder = (tn, fmt) => s"./data/sparKafka/${tn.value}/${fmt.alias}.${fmt.format}",
      datePartitionPathBuilder = partitionPath,
      saveMode = SaveMode.ErrorIfExists,
      locationStrategy = LocationStrategies.PreferConsistent,
      showDs = NJShowDataset(20, isTruncate = false)
    )
  }
}

@deriveFixedPoint sealed private[kafka] trait SKConfigF[_]

private[kafka] object SKConfigF {
  final case class DefaultParams[K](topicName: TopicName, zoneId: ZoneId) extends SKConfigF[K]

  final case class WithTopicName[K](value: TopicName, cont: K) extends SKConfigF[K]
  final case class WithBatchSize[K](value: Int, cont: K) extends SKConfigF[K]
  final case class WithDuration[K](value: FiniteDuration, cont: K) extends SKConfigF[K]
  final case class WithUploadRecordsLimit[K](value: Long, cont: K) extends SKConfigF[K]
  final case class WithUploadTimeLimit[K](value: FiniteDuration, cont: K) extends SKConfigF[K]

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

  final case class WithReplayPathBuilder[K](value: TopicName => String, cont: K)
      extends SKConfigF[K]

  final case class WithPathBuilder[K](value: (TopicName, NJFileFormat) => String, cont: K)
      extends SKConfigF[K]

  final case class WithDatePartitionPathBuilder[K](
    value: (TopicName, NJFileFormat, LocalDate) => String,
    cont: K)
      extends SKConfigF[K]

  private val algebra: Algebra[SKConfigF, SKParams] = Algebra[SKConfigF, SKParams] {
    case DefaultParams(t, z) => SKParams(t, z)
    case WithTopicName(v, c) => SKParams.topicName.set(v)(c)
    case WithBatchSize(v, c) =>
      SKParams.uploadParams.composeLens(NJUploadParams.batchSize).set(v)(c)
    case WithDuration(v, c) => SKParams.uploadParams.composeLens(NJUploadParams.duration).set(v)(c)
    case WithUploadRecordsLimit(v, c) =>
      SKParams.uploadParams.composeLens(NJUploadParams.recordsLimit).set(v)(c)
    case WithUploadTimeLimit(v, c) =>
      SKParams.uploadParams.composeLens(NJUploadParams.timeLimit).set(v)(c)
    case WithStartTimeStr(v, c)             => SKParams.timeRange.modify(_.withStartTime(v))(c)
    case WithEndTimeStr(v, c)               => SKParams.timeRange.modify(_.withEndTime(v))(c)
    case WithStartTime(v, c)                => SKParams.timeRange.modify(_.withStartTime(v))(c)
    case WithEndTime(v, c)                  => SKParams.timeRange.modify(_.withEndTime(v))(c)
    case WithZoneId(v, c)                   => SKParams.timeRange.modify(_.withZoneId(v))(c)
    case WithTimeRange(v, c)                => SKParams.timeRange.set(v)(c)
    case WithNSeconds(v, c)                 => SKParams.timeRange.modify(_.withNSeconds(v))(c)
    case WithOneDay(v, c)                   => SKParams.timeRange.modify(_.withOneDay(v))(c)
    case WithOneDayStr(v, c)                => SKParams.timeRange.modify(_.withOneDay(v))(c)
    case WithSaveMode(v, c)                 => SKParams.saveMode.set(v)(c)
    case WithLocationStrategy(v, c)         => SKParams.locationStrategy.set(v)(c)
    case WithShowRows(v, c)                 => SKParams.showDs.composeLens(NJShowDataset.rowNum).set(v)(c)
    case WithShowTruncate(v, c)             => SKParams.showDs.composeLens(NJShowDataset.isTruncate).set(v)(c)
    case WithReplayPathBuilder(v, c)        => SKParams.replayPathBuilder.set(v)(c)
    case WithPathBuilder(v, c)              => SKParams.pathBuilder.set(v)(c)
    case WithDatePartitionPathBuilder(v, c) => SKParams.datePartitionPathBuilder.set(v)(c)
  }

  def evalConfig(cfg: SKConfig): SKParams = scheme.cata(algebra).apply(cfg.value)
}

final private[kafka] case class SKConfig private (value: Fix[SKConfigF]) extends AnyVal {
  import SKConfigF._

  def withTopicName(tn: String): SKConfig =
    SKConfig(Fix(WithTopicName(TopicName.unsafeFrom(tn), value)))

  def withBatchSize(bs: Int): SKConfig           = SKConfig(Fix(WithBatchSize(bs, value)))
  def withDuration(fd: FiniteDuration): SKConfig = SKConfig(Fix(WithDuration(fd, value)))
  def withDuration(ms: Long): SKConfig           = withDuration(FiniteDuration(ms, TimeUnit.MILLISECONDS))

  def withUploadRecordsLimit(num: Long): SKConfig = SKConfig(
    Fix(WithUploadRecordsLimit(num, value)))

  def withUploadTimeLimit(fd: FiniteDuration): SKConfig = SKConfig(
    Fix(WithUploadTimeLimit(fd, value)))

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

  def withReplayPathBuilder(f: TopicName => String): SKConfig =
    SKConfig(Fix(WithReplayPathBuilder(f, value)))

  def withPathBuilder(f: (TopicName, NJFileFormat) => String): SKConfig =
    SKConfig(Fix(WithPathBuilder(f, value)))

  def withDatePartitionBuilder(f: (TopicName, NJFileFormat, LocalDate) => String): SKConfig =
    SKConfig(Fix(WithDatePartitionPathBuilder(f, value)))

  def evalConfig: SKParams = SKConfigF.evalConfig(this)
}

private[spark] object SKConfig {

  def apply(topicName: TopicName, zoneId: ZoneId): SKConfig =
    SKConfig(Fix(SKConfigF.DefaultParams[Fix[SKConfigF]](topicName, zoneId)))

  def apply(topicName: TopicName, dtr: NJDateTimeRange): SKConfig =
    apply(topicName, dtr.zoneId).withTimeRange(dtr)
}
