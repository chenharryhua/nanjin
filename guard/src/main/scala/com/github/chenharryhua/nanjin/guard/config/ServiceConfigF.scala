package com.github.chenharryhua.nanjin.guard.config

import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.guard.{QueueCapacity, ServiceName}
import cron4s.{Cron, CronExpr}
import cron4s.circe.*
import cron4s.lib.javatime.javaTemporalInstance
import eu.timepit.refined.cats.*
import eu.timepit.refined.refineMV
import higherkindness.droste.{scheme, Algebra}
import higherkindness.droste.data.Fix
import io.circe.{Decoder, Encoder}
import io.circe.generic.JsonCodec
import io.circe.refined.*
import io.scalaland.enumz.Enum
import monocle.macros.Lenses
import org.typelevel.cats.time.instances.zoneddatetime

import java.time.*
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.ScalaDurationOps

@Lenses @JsonCodec final case class MetricParams private[guard] (
  reportSchedule: Option[ScheduleType],
  resetSchedule: Option[CronExpr],
  rateTimeUnit: TimeUnit,
  durationTimeUnit: TimeUnit,
  snapshotType: MetricSnapshotType) {
  def nextReport(now: ZonedDateTime): Option[ZonedDateTime] =
    reportSchedule.flatMap(_.fold(fd => Some(now.plus(fd)), _.next(now)))
  def nextReset(now: ZonedDateTime): Option[ZonedDateTime] =
    resetSchedule.flatMap(_.next(now))
}

private[guard] object MetricParams {
  private[this] val enumTimeUnit: Enum[TimeUnit]        = Enum[TimeUnit]
  implicit final val encoderTimeUnit: Encoder[TimeUnit] = Encoder.encodeString.contramap(enumTimeUnit.getName)
  implicit final val decoderTimeUnit: Decoder[TimeUnit] = Decoder.decodeString.map(enumTimeUnit.withName)
  implicit final val showTimeUnit: Show[TimeUnit]       = enumTimeUnit.getName

  implicit val showMetricParams: Show[MetricParams] = cats.derived.semiauto.show[MetricParams]
}

@Lenses @JsonCodec final case class ServiceParams private (
  serviceName: ServiceName,
  taskParams: TaskParams,
  retry: NJRetryPolicy,
  queueCapacity: QueueCapacity,
  metric: MetricParams,
  brief: String,
  serviceID: UUID,
  launchTime: ZonedDateTime
) {
  def toZonedDateTime(ts: Instant): ZonedDateTime = ts.atZone(taskParams.zoneId)
  def toLocalDateTime(ts: Instant): LocalDateTime = toZonedDateTime(ts).toLocalDateTime
  def toLocalDate(ts: Instant): LocalDate         = toZonedDateTime(ts).toLocalDate
  def toLocalTime(ts: Instant): LocalTime         = toZonedDateTime(ts).toLocalTime
  def upTime(ts: ZonedDateTime): Duration         = Duration.between(launchTime, ts)
  def upTime(ts: Instant): Duration               = Duration.between(launchTime, ts)
}

private[guard] object ServiceParams extends zoneddatetime {

  implicit val showServiceParams: Show[ServiceParams] = cats.derived.semiauto.show[ServiceParams]

  def apply(
    serviceName: ServiceName,
    taskParams: TaskParams,
    serviceID: UUID,
    launchTime: Instant): ServiceParams =
    ServiceParams(
      serviceName = serviceName,
      taskParams = taskParams,
      retry = NJRetryPolicy.ConstantDelay(30.seconds.toJava),
      queueCapacity = refineMV(0), // synchronous
      metric = MetricParams(
        reportSchedule = None,
        resetSchedule = None,
        rateTimeUnit = TimeUnit.SECONDS,
        durationTimeUnit = TimeUnit.MILLISECONDS,
        snapshotType = MetricSnapshotType.Regular
      ),
      brief = "no brief",
      serviceID = serviceID,
      launchTime = launchTime.atZone(taskParams.zoneId)
    )
}

sealed private[guard] trait ServiceConfigF[X]

private object ServiceConfigF {
  implicit val functorServiceConfigF: Functor[ServiceConfigF] = cats.derived.semiauto.functor[ServiceConfigF]

  final case class InitParams[K](serviceName: ServiceName, taskParams: TaskParams) extends ServiceConfigF[K]
  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends ServiceConfigF[K]
  final case class WithServiceName[K](value: ServiceName, cont: K) extends ServiceConfigF[K]
  final case class WithQueueCapacity[K](value: QueueCapacity, cont: K) extends ServiceConfigF[K]

  final case class WithReportSchedule[K](value: Option[ScheduleType], cont: K) extends ServiceConfigF[K]
  final case class WithResetSchedule[K](value: Option[CronExpr], cont: K) extends ServiceConfigF[K]
  final case class WithRateTimeUnit[K](value: TimeUnit, cont: K) extends ServiceConfigF[K]
  final case class WithDurationTimeUnit[K](value: TimeUnit, cont: K) extends ServiceConfigF[K]

  final case class WithSnapshotType[K](value: MetricSnapshotType, cont: K) extends ServiceConfigF[K]

  final case class WithBrief[K](value: String, cont: K) extends ServiceConfigF[K]

  def algebra(serviceID: UUID, launchTime: Instant): Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams(s, t)        => ServiceParams(s, t, serviceID, launchTime)
      case WithRetryPolicy(v, c)   => ServiceParams.retry.set(v)(c)
      case WithServiceName(v, c)   => ServiceParams.serviceName.set(v)(c)
      case WithQueueCapacity(v, c) => ServiceParams.queueCapacity.set(v)(c)

      case WithReportSchedule(v, c) => ServiceParams.metric.composeLens(MetricParams.reportSchedule).set(v)(c)
      case WithResetSchedule(v, c)  => ServiceParams.metric.composeLens(MetricParams.resetSchedule).set(v)(c)
      case WithRateTimeUnit(v, c)   => ServiceParams.metric.composeLens(MetricParams.rateTimeUnit).set(v)(c)
      case WithDurationTimeUnit(v, c) =>
        ServiceParams.metric.composeLens(MetricParams.durationTimeUnit).set(v)(c)
      case WithSnapshotType(v, c) => ServiceParams.metric.composeLens(MetricParams.snapshotType).set(v)(c)

      case WithBrief(v, c) => ServiceParams.brief.set(v)(c)

    }
}

final case class ServiceConfig private (value: Fix[ServiceConfigF]) {
  import ServiceConfigF.*

  def withQueueCapacity(size: QueueCapacity): ServiceConfig =
    ServiceConfig(Fix(WithQueueCapacity(size, value)))

  def withServiceName(name: ServiceName): ServiceConfig = ServiceConfig(Fix(WithServiceName(name, value)))

  def withBrief(text: String): ServiceConfig = ServiceConfig(Fix(WithBrief(text, value)))

  // metrics
  def withMetricReport(interval: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithReportSchedule(Some(ScheduleType.Fixed(interval.toJava)), value)))

  def withMetricReport(crontab: CronExpr): ServiceConfig =
    ServiceConfig(Fix(WithReportSchedule(Some(ScheduleType.Cron(crontab)), value)))

  def withMetricReport(crontab: String): ServiceConfig =
    withMetricReport(Cron.unsafeParse(crontab))

  def withMetricReset(crontab: CronExpr): ServiceConfig =
    ServiceConfig(Fix(WithResetSchedule(Some(crontab), value)))
  def withMetricReset(crontab: String): ServiceConfig = withMetricReset(Cron.unsafeParse(crontab))
  def withMetricDailyReset: ServiceConfig             = withMetricReset(Cron.unsafeParse("1 0 0 ? * *"))
  def withMetricWeeklyReset: ServiceConfig            = withMetricReset(Cron.unsafeParse("1 0 0 ? * 0"))
  def withMetricMonthlyReset: ServiceConfig           = withMetricReset(Cron.unsafeParse("1 0 0 1 * ?"))

  def withMetricRateTimeUnit(tu: TimeUnit): ServiceConfig = ServiceConfig(Fix(WithRateTimeUnit(tu, value)))
  def withMetricDurationTimeUnit(tu: TimeUnit): ServiceConfig = ServiceConfig(
    Fix(WithDurationTimeUnit(tu, value)))

  def withMetricSnapshotType(mst: MetricSnapshotType): ServiceConfig =
    ServiceConfig(Fix(WithSnapshotType(mst, value)))

  // retries
  def withConstantDelay(baseDelay: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithRetryPolicy(NJRetryPolicy.ConstantDelay(baseDelay.toJava), value)))

  def withJitterBackoff(minDelay: FiniteDuration, maxDelay: FiniteDuration): ServiceConfig = {
    require(maxDelay > minDelay, s"maxDelay($maxDelay) should be strictly bigger than minDelay($minDelay)")
    ServiceConfig(Fix(WithRetryPolicy(NJRetryPolicy.JitterBackoff(minDelay.toJava, maxDelay.toJava), value)))
  }

  def withJitterBackoff(maxDelay: FiniteDuration): ServiceConfig =
    withJitterBackoff(FiniteDuration(0, TimeUnit.SECONDS), maxDelay)

  def withAlwaysGiveUp: ServiceConfig =
    ServiceConfig(Fix(WithRetryPolicy(NJRetryPolicy.AlwaysGiveUp, value)))

  def evalConfig(serviceID: UUID, launchTime: Instant): ServiceParams =
    scheme.cata(algebra(serviceID, launchTime)).apply(value)
}

private[guard] object ServiceConfig {

  def apply(serviceName: ServiceName, taskParams: TaskParams): ServiceConfig =
    new ServiceConfig(Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](serviceName, taskParams)))
}
