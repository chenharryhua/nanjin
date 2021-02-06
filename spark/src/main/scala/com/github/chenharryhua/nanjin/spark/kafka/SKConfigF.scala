package com.github.chenharryhua.nanjin.spark.kafka

import cats.derived.auto.functor.kittensMkFunctor
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.{defaultLoadParams, TopicName}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.streaming.kafka010.{LocationStrategies, LocationStrategy}

import java.time.{LocalDate, LocalDateTime, ZoneId}
import scala.concurrent.duration._

@Lenses final private[kafka] case class NJLoadParams(
  bulkSize: Int,
  uploadBatchSize: Int,
  interval: FiniteDuration,
  recordsLimit: Long,
  timeLimit: FiniteDuration,
  bufferSize: Int,
  idleTimeout: FiniteDuration)

private[kafka] object NJLoadParams {

  val default: NJLoadParams = NJLoadParams(
    bulkSize = defaultLoadParams.BulkSize,
    uploadBatchSize = defaultLoadParams.BatchSize,
    interval = defaultLoadParams.Interval,
    recordsLimit = defaultLoadParams.RecordsLimit,
    timeLimit = defaultLoadParams.TimeLimit,
    bufferSize = defaultLoadParams.BufferSize,
    idleTimeout = defaultLoadParams.IdleTimeout
  )
}

@Lenses final private[kafka] case class SKParams private (
  topicName: TopicName,
  timeRange: NJDateTimeRange,
  locationStrategy: LocationStrategy,
  replayPathBuilder: TopicName => String,
  loadParams: NJLoadParams) {
  val replayPath: String = replayPathBuilder(topicName)
}

private[kafka] object SKParams {

  def apply(topicName: TopicName, zoneId: ZoneId): SKParams =
    SKParams(
      topicName = topicName,
      timeRange = NJDateTimeRange(zoneId),
      locationStrategy = LocationStrategies.PreferConsistent,
      replayPathBuilder = topicName => s"./data/sparKafka/${topicName.value}/replay/",
      loadParams = NJLoadParams.default
    )
}

sealed private[kafka] trait SKConfigF[A]

private[kafka] object SKConfigF {
  final case class InitParams[K](topicName: TopicName, zoneId: ZoneId) extends SKConfigF[K]

  final case class WithTopicName[K](value: TopicName, cont: K) extends SKConfigF[K]

  final case class WithLoadBulkSize[K](value: Int, cont: K) extends SKConfigF[K]
  final case class WithUploadLoadBatchSize[K](value: Int, cont: K) extends SKConfigF[K]
  final case class WithLoadInterval[K](value: FiniteDuration, cont: K) extends SKConfigF[K]
  final case class WithLoadRecordsLimit[K](value: Long, cont: K) extends SKConfigF[K]
  final case class WithLoadTimeLimit[K](value: FiniteDuration, cont: K) extends SKConfigF[K]
  final case class WithLoadBufferSize[K](value: Int, cont: K) extends SKConfigF[K]
  final case class WithIdleTimeout[K](value: FiniteDuration, cont: K) extends SKConfigF[K]

  final case class WithStartTimeStr[K](value: String, cont: K) extends SKConfigF[K]
  final case class WithStartTime[K](value: LocalDateTime, cont: K) extends SKConfigF[K]
  final case class WithEndTimeStr[K](value: String, cont: K) extends SKConfigF[K]
  final case class WithEndTime[K](value: LocalDateTime, cont: K) extends SKConfigF[K]
  final case class WithZoneId[K](value: ZoneId, cont: K) extends SKConfigF[K]
  final case class WithTimeRange[K](value: NJDateTimeRange, cont: K) extends SKConfigF[K]
  final case class WithNSeconds[K](value: Long, cont: K) extends SKConfigF[K]
  final case class WithOneDay[K](value: LocalDate, cont: K) extends SKConfigF[K]
  final case class WithOneDayStr[K](value: String, cont: K) extends SKConfigF[K]

  final case class WithLocationStrategy[K](value: LocationStrategy, cont: K) extends SKConfigF[K]

  final case class WithReplayPathBuilder[K](value: TopicName => String, cont: K) extends SKConfigF[K]

  private val algebra: Algebra[SKConfigF, SKParams] = Algebra[SKConfigF, SKParams] {
    case InitParams(t, z)    => SKParams(t, z)
    case WithTopicName(v, c) => SKParams.topicName.set(v)(c)

    case WithLoadBulkSize(v, c)     => SKParams.loadParams.composeLens(NJLoadParams.bulkSize).set(v)(c)
    case WithLoadInterval(v, c)     => SKParams.loadParams.composeLens(NJLoadParams.interval).set(v)(c)
    case WithLoadRecordsLimit(v, c) => SKParams.loadParams.composeLens(NJLoadParams.recordsLimit).set(v)(c)
    case WithLoadTimeLimit(v, c)    => SKParams.loadParams.composeLens(NJLoadParams.timeLimit).set(v)(c)
    case WithLoadBufferSize(v, c)   => SKParams.loadParams.composeLens(NJLoadParams.bufferSize).set(v)(c)
    case WithIdleTimeout(v, c)      => SKParams.loadParams.composeLens(NJLoadParams.idleTimeout).set(v)(c)

    case WithUploadLoadBatchSize(v, c) => SKParams.loadParams.composeLens(NJLoadParams.uploadBatchSize).set(v)(c)

    case WithStartTimeStr(v, c) => SKParams.timeRange.modify(_.withStartTime(v))(c)
    case WithEndTimeStr(v, c)   => SKParams.timeRange.modify(_.withEndTime(v))(c)
    case WithStartTime(v, c)    => SKParams.timeRange.modify(_.withStartTime(v))(c)
    case WithEndTime(v, c)      => SKParams.timeRange.modify(_.withEndTime(v))(c)
    case WithZoneId(v, c)       => SKParams.timeRange.modify(_.withZoneId(v))(c)
    case WithTimeRange(v, c)    => SKParams.timeRange.set(v)(c)
    case WithNSeconds(v, c)     => SKParams.timeRange.modify(_.withNSeconds(v))(c)
    case WithOneDay(v, c)       => SKParams.timeRange.modify(_.withOneDay(v))(c)
    case WithOneDayStr(v, c)    => SKParams.timeRange.modify(_.withOneDay(v))(c)

    case WithLocationStrategy(v, c)  => SKParams.locationStrategy.set(v)(c)
    case WithReplayPathBuilder(v, c) => SKParams.replayPathBuilder.set(v)(c)
  }

  def evalConfig(cfg: SKConfig): SKParams = scheme.cata(algebra).apply(cfg.value)
}

final private[kafka] case class SKConfig private (value: Fix[SKConfigF]) extends AnyVal {
  import SKConfigF._

  def withTopicName(tn: String): SKConfig = SKConfig(Fix(WithTopicName(TopicName.unsafeFrom(tn), value)))

  def withLoadBulkSize(bs: Int): SKConfig               = SKConfig(Fix(WithLoadBulkSize(bs, value)))
  def withLoadInterval(fd: FiniteDuration): SKConfig    = SKConfig(Fix(WithLoadInterval(fd, value)))
  def withLoadRecordsLimit(num: Long): SKConfig         = SKConfig(Fix(WithLoadRecordsLimit(num, value)))
  def withLoadTimeLimit(fd: FiniteDuration): SKConfig   = SKConfig(Fix(WithLoadTimeLimit(fd, value)))
  def withLoadBufferSize(num: Int): SKConfig            = SKConfig(Fix(WithLoadBufferSize(num, value)))
  def withLoadIdleTimeout(fd: FiniteDuration): SKConfig = SKConfig(Fix(WithIdleTimeout(fd, value)))

  def withUploadBatchSize(num: Int): SKConfig = SKConfig(Fix(WithUploadLoadBatchSize(num, value)))

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

  def withLocationStrategy(ls: LocationStrategy): SKConfig = SKConfig(Fix(WithLocationStrategy(ls, value)))

  def withReplayPathBuilder(f: TopicName => String): SKConfig = SKConfig(Fix(WithReplayPathBuilder(f, value)))

  def evalConfig: SKParams = SKConfigF.evalConfig(this)
}

private[spark] object SKConfig {

  def apply(topicName: TopicName, zoneId: ZoneId): SKConfig =
    SKConfig(Fix(SKConfigF.InitParams[Fix[SKConfigF]](topicName, zoneId)))

}
