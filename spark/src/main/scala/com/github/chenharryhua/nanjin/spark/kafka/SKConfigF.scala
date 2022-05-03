package com.github.chenharryhua.nanjin.spark.kafka

import cats.Functor
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.PathSegment
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import org.apache.spark.streaming.kafka010.{LocationStrategies, LocationStrategy}

import java.time.{LocalDate, LocalDateTime, ZoneId}

@Lenses final private[kafka] case class SKParams private (
  topicName: TopicName,
  timeRange: NJDateTimeRange,
  locationStrategy: LocationStrategy,
  replayPathBuilder: TopicName => NJPath) {
  val replayPath: NJPath = replayPathBuilder(topicName)
}

private[kafka] object SKParams {

  def apply(topicName: TopicName, zoneId: ZoneId): SKParams =
    SKParams(
      topicName = topicName,
      timeRange = NJDateTimeRange(zoneId),
      locationStrategy = LocationStrategies.PreferConsistent,
      replayPathBuilder = topicName => NJPath("data/sparKafka") / PathSegment.unsafeFrom(topicName.value) / "replay"
    )
}

sealed private[kafka] trait SKConfigF[X]

private object SKConfigF {
  implicit val functorSKConfigF: Functor[SKConfigF] = cats.derived.semiauto.functor[SKConfigF]

  final case class InitParams[K](topicName: TopicName, zoneId: ZoneId) extends SKConfigF[K]

  final case class WithTopicName[K](value: TopicName, cont: K) extends SKConfigF[K]

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
