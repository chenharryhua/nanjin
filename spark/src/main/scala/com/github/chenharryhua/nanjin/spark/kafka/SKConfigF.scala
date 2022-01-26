package com.github.chenharryhua.nanjin.spark.kafka

import cats.Functor
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.{ChunkSize, PathSegment}
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.spark
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.streaming.kafka010.{LocationStrategies, LocationStrategy}
import squants.information.{Gigabytes, Information}

import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

@Lenses final private[kafka] case class NJLoadParams(
  throttle: Information, // maximum byte per interval
  chunkSize: ChunkSize,
  byteBuffer: Information,
  interval: FiniteDuration,
  recordsLimit: Long,
  timeLimit: FiniteDuration,
  idleTimeout: FiniteDuration)

private[kafka] object NJLoadParams {

  val default: NJLoadParams = NJLoadParams(
    throttle = Gigabytes(1), // akka maximum 1gb/second
    chunkSize = spark.chunkSize,
    byteBuffer = spark.byteBuffer,
    interval = FiniteDuration(1, TimeUnit.SECONDS),
    recordsLimit = Long.MaxValue,
    timeLimit = FiniteDuration(21474835, TimeUnit.SECONDS), // akka.actor.LightArrayRevolverScheduler.checkMaxDelay
    idleTimeout = FiniteDuration(120, TimeUnit.SECONDS)
  )
}

@Lenses final private[kafka] case class SKParams private (
  topicName: TopicName,
  timeRange: NJDateTimeRange,
  locationStrategy: LocationStrategy,
  replayPathBuilder: TopicName => NJPath,
  loadParams: NJLoadParams) {
  val replayPath: NJPath = replayPathBuilder(topicName)
}

private[kafka] object SKParams {

  def apply(topicName: TopicName, zoneId: ZoneId): SKParams =
    SKParams(
      topicName = topicName,
      timeRange = NJDateTimeRange(zoneId),
      locationStrategy = LocationStrategies.PreferConsistent,
      replayPathBuilder = topicName => NJPath("data/sparKafka") / PathSegment.unsafeFrom(topicName.value) / "replay",
      loadParams = NJLoadParams.default
    )
}

sealed private[kafka] trait SKConfigF[A]

private object SKConfigF {
  implicit val functorSKConfigF: Functor[SKConfigF] = cats.derived.semiauto.functor[SKConfigF]

  final case class InitParams[K](topicName: TopicName, zoneId: ZoneId) extends SKConfigF[K]

  final case class WithTopicName[K](value: TopicName, cont: K) extends SKConfigF[K]

  final case class WithLoadThrottle[K](value: Information, cont: K) extends SKConfigF[K]
  final case class WithLoadChunkSize[K](value: ChunkSize, cont: K) extends SKConfigF[K]
  final case class WithBufferSize[K](value: Information, cont: K) extends SKConfigF[K]
  final case class WithLoadInterval[K](value: FiniteDuration, cont: K) extends SKConfigF[K]
  final case class WithLoadRecordsLimit[K](value: Long, cont: K) extends SKConfigF[K]
  final case class WithLoadTimeLimit[K](value: FiniteDuration, cont: K) extends SKConfigF[K]
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

  final case class WithReplayPathBuilder[K](value: TopicName => NJPath, cont: K) extends SKConfigF[K]

  private val algebra: Algebra[SKConfigF, SKParams] = Algebra[SKConfigF, SKParams] {
    case InitParams(t, z)    => SKParams(t, z)
    case WithTopicName(v, c) => SKParams.topicName.set(v)(c)

    case WithLoadThrottle(v, c)     => SKParams.loadParams.composeLens(NJLoadParams.throttle).set(v)(c)
    case WithLoadInterval(v, c)     => SKParams.loadParams.composeLens(NJLoadParams.interval).set(v)(c)
    case WithLoadRecordsLimit(v, c) => SKParams.loadParams.composeLens(NJLoadParams.recordsLimit).set(v)(c)
    case WithLoadTimeLimit(v, c)    => SKParams.loadParams.composeLens(NJLoadParams.timeLimit).set(v)(c)
    case WithLoadChunkSize(v, c)    => SKParams.loadParams.composeLens(NJLoadParams.chunkSize).set(v)(c)
    case WithBufferSize(v, c)       => SKParams.loadParams.composeLens(NJLoadParams.byteBuffer).set(v)(c)

    case WithIdleTimeout(v, c) => SKParams.loadParams.composeLens(NJLoadParams.idleTimeout).set(v)(c)

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
  import SKConfigF.*

  def topicName(tn: String): SKConfig = SKConfig(Fix(WithTopicName(TopicName.unsafeFrom(tn), value)))

  def loadThrottle(bytes: Information): SKConfig    = SKConfig(Fix(WithLoadThrottle(bytes, value)))
  def loadInterval(fd: FiniteDuration): SKConfig    = SKConfig(Fix(WithLoadInterval(fd, value)))
  def loadRecordsLimit(num: Long): SKConfig         = SKConfig(Fix(WithLoadRecordsLimit(num, value)))
  def loadTimeLimit(fd: FiniteDuration): SKConfig   = SKConfig(Fix(WithLoadTimeLimit(fd, value)))
  def loadIdleTimeout(fd: FiniteDuration): SKConfig = SKConfig(Fix(WithIdleTimeout(fd, value)))
  def loadChunkSize(num: ChunkSize): SKConfig       = SKConfig(Fix(WithLoadChunkSize(num, value)))
  def loadByteBuffer(bb: Information): SKConfig     = SKConfig(Fix(WithBufferSize(bb, value)))

  def startTime(s: String): SKConfig                  = SKConfig(Fix(WithStartTimeStr(s, value)))
  def startTime(s: LocalDateTime): SKConfig           = SKConfig(Fix(WithStartTime(s, value)))
  def endTime(s: String): SKConfig                    = SKConfig(Fix(WithEndTimeStr(s, value)))
  def endTime(s: LocalDateTime): SKConfig             = SKConfig(Fix(WithEndTime(s, value)))
  def zoneId(s: ZoneId): SKConfig                     = SKConfig(Fix(WithZoneId(s, value)))
  def timeRange(tr: NJDateTimeRange): SKConfig        = SKConfig(Fix(WithTimeRange(tr, value)))
  def timeRange(start: String, end: String): SKConfig = startTime(start).endTime(end)
  def timeRangeNSeconds(s: Long): SKConfig            = SKConfig(Fix(WithNSeconds(s, value)))
  def timeRangeOneDay(s: String): SKConfig            = SKConfig(Fix(WithOneDayStr(s, value)))
  def timeRangeOneDay(s: LocalDate): SKConfig         = SKConfig(Fix(WithOneDay(s, value)))
  def timeRangeToday: SKConfig                        = timeRangeOneDay(LocalDate.now)
  def timeRangeYesterday: SKConfig                    = timeRangeOneDay(LocalDate.now.minusDays(1))

  def locationStrategy(ls: LocationStrategy): SKConfig = SKConfig(Fix(WithLocationStrategy(ls, value)))

  def replayPathBuilder(f: TopicName => NJPath): SKConfig = SKConfig(Fix(WithReplayPathBuilder(f, value)))

  def evalConfig: SKParams = SKConfigF.evalConfig(this)
}

private[spark] object SKConfig {

  def apply(topicName: TopicName, zoneId: ZoneId): SKConfig =
    SKConfig(Fix(SKConfigF.InitParams[Fix[SKConfigF]](topicName, zoneId)))

}
