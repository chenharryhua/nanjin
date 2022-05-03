package com.github.chenharryhua.nanjin.guard.config

import cats.derived.auto.show.*
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.datetime.instances.*
import cron4s.lib.javatime.javaTemporalInstance
import cron4s.{Cron, CronExpr}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.cats.*
import eu.timepit.refined.refineMV
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import io.circe.refined.*
import monocle.macros.Lenses

import java.time.*
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.ScalaDurationOps

@Lenses @JsonCodec final case class MetricParams private[guard] (
  reportSchedule: Option[Either[FiniteDuration, CronExpr]],
  resetSchedule: Option[CronExpr],
  rateTimeUnit: TimeUnit,
  durationTimeUnit: TimeUnit,
  snapshotType: MetricSnapshotType) {
  def nextReport(now: ZonedDateTime): Option[ZonedDateTime] =
    reportSchedule.flatMap(_.fold(fd => Some(now.plus(fd.toJava)), _.next(now)))
  def nextReset(now: ZonedDateTime): Option[ZonedDateTime] =
    resetSchedule.flatMap(_.next(now))
}

private[guard] object MetricParams {
  implicit val showMetricParams: Show[MetricParams] = cats.derived.semiauto.show[MetricParams]
}

@Lenses @JsonCodec final case class ServiceParams private (
  serviceName: ServiceName,
  taskParams: TaskParams,
  retry: NJRetryPolicy,
  queueCapacity: QueueCapacity,
  metric: MetricParams,
  brief: String
) {
  val metricName: Digested                        = Digested(serviceName, taskParams)
  def toZonedDateTime(ts: Instant): ZonedDateTime = ts.atZone(taskParams.zoneId)
  def toLocalDateTime(ts: Instant): LocalDateTime = toZonedDateTime(ts).toLocalDateTime
  def toLocalDate(ts: Instant): LocalDate         = toZonedDateTime(ts).toLocalDate
  def toLocalTime(ts: Instant): LocalTime         = toZonedDateTime(ts).toLocalTime
}

private[guard] object ServiceParams {

  implicit val showServiceParams: Show[ServiceParams] = cats.derived.semiauto.show[ServiceParams]

  def apply(serviceName: ServiceName, taskParams: TaskParams): ServiceParams =
    ServiceParams(
      serviceName = serviceName,
      taskParams = taskParams,
      retry = NJRetryPolicy.ConstantDelay(30.seconds),
      queueCapacity = refineMV(0), // synchronous
      metric = MetricParams(
        reportSchedule = None,
        resetSchedule = None,
        rateTimeUnit = TimeUnit.SECONDS,
        durationTimeUnit = TimeUnit.MILLISECONDS,
        snapshotType = MetricSnapshotType.Regular
      ),
      brief = "no brief"
    )
}

sealed private[guard] trait ServiceConfigF[X]

private object ServiceConfigF {
  implicit val functorServiceConfigF: Functor[ServiceConfigF] = cats.derived.semiauto.functor[ServiceConfigF]

  final case class InitParams[K](serviceName: ServiceName, taskParams: TaskParams) extends ServiceConfigF[K]
  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends ServiceConfigF[K]
  final case class WithServiceName[K](value: ServiceName, cont: K) extends ServiceConfigF[K]
  final case class WithQueueCapacity[K](value: QueueCapacity, cont: K) extends ServiceConfigF[K]

  final case class WithReportSchedule[K](value: Option[Either[FiniteDuration, CronExpr]], cont: K)
      extends ServiceConfigF[K]
  final case class WithResetSchedule[K](value: Option[CronExpr], cont: K) extends ServiceConfigF[K]
  final case class WithRateTimeUnit[K](value: TimeUnit, cont: K) extends ServiceConfigF[K]
  final case class WithDurationTimeUnit[K](value: TimeUnit, cont: K) extends ServiceConfigF[K]

  final case class WithSnapshotType[K](value: MetricSnapshotType, cont: K) extends ServiceConfigF[K]

  final case class WithBrief[K](value: String, cont: K) extends ServiceConfigF[K]

  val algebra: Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams(s, t)        => ServiceParams(s, t)
      case WithRetryPolicy(v, c)   => ServiceParams.retry.set(v)(c)
      case WithServiceName(v, c)   => ServiceParams.serviceName.set(v)(c)
      case WithQueueCapacity(v, c) => ServiceParams.queueCapacity.set(v)(c)

      case WithReportSchedule(v, c)   => ServiceParams.metric.composeLens(MetricParams.reportSchedule).set(v)(c)
      case WithResetSchedule(v, c)    => ServiceParams.metric.composeLens(MetricParams.resetSchedule).set(v)(c)
      case WithRateTimeUnit(v, c)     => ServiceParams.metric.composeLens(MetricParams.rateTimeUnit).set(v)(c)
      case WithDurationTimeUnit(v, c) => ServiceParams.metric.composeLens(MetricParams.durationTimeUnit).set(v)(c)
      case WithSnapshotType(v, c)     => ServiceParams.metric.composeLens(MetricParams.snapshotType).set(v)(c)

      case WithBrief(v, c) => ServiceParams.brief.set(v)(c)

    }
}

final case class ServiceConfig private (value: Fix[ServiceConfigF]) {
  import ServiceConfigF.*

  def withQueueCapacity(size: QueueCapacity): ServiceConfig = ServiceConfig(Fix(WithQueueCapacity(size, value)))
  def withServiceName(name: ServiceName): ServiceConfig     = ServiceConfig(Fix(WithServiceName(name, value)))

  def withMetricReport(interval: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithReportSchedule(Some(Left(interval)), value)))

  def withMetricReport(crontab: CronExpr): ServiceConfig =
    ServiceConfig(Fix(WithReportSchedule(Some(Right(crontab)), value)))

  def withMetricReport(crontab: String): ServiceConfig =
    withMetricReport(Cron.unsafeParse(crontab))

  def withMetricReset(crontab: CronExpr): ServiceConfig = ServiceConfig(Fix(WithResetSchedule(Some(crontab), value)))
  def withMetricReset(crontab: String): ServiceConfig   = withMetricReset(Cron.unsafeParse(crontab))
  def withMetricDailyReset: ServiceConfig               = withMetricReset(Cron.unsafeParse("1 0 0 ? * *"))
  def withMetricWeeklyReset: ServiceConfig              = withMetricReset(Cron.unsafeParse("1 0 0 ? * 0"))
  def withMetricMonthlyReset: ServiceConfig             = withMetricReset(Cron.unsafeParse("1 0 0 1 * ?"))

  def withMetricRateTimeUnit(tu: TimeUnit): ServiceConfig     = ServiceConfig(Fix(WithRateTimeUnit(tu, value)))
  def withMetricDurationTimeUnit(tu: TimeUnit): ServiceConfig = ServiceConfig(Fix(WithDurationTimeUnit(tu, value)))

  def withMetricSnapshotType(mst: MetricSnapshotType): ServiceConfig = ServiceConfig(Fix(WithSnapshotType(mst, value)))

  def withConstantDelay(delay: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithRetryPolicy(NJRetryPolicy.ConstantDelay(delay), value)))

  def withJitterBackoff(minDelay: FiniteDuration, maxDelay: FiniteDuration): ServiceConfig = {
    require(maxDelay > minDelay, s"maxDelay($maxDelay) should be strictly bigger than minDelay($minDelay)")
    ServiceConfig(Fix(WithRetryPolicy(NJRetryPolicy.JitterBackoff(minDelay, maxDelay), value)))
  }

  def withJitterBackoff(maxDelay: FiniteDuration): ServiceConfig =
    withJitterBackoff(FiniteDuration(0, TimeUnit.SECONDS), maxDelay)

  def withBrief(text: String): ServiceConfig = ServiceConfig(Fix(WithBrief(text, value)))

  def evalConfig: ServiceParams = scheme.cata(algebra).apply(value)
}

private[guard] object ServiceConfig {

  def apply(serviceName: ServiceName, taskParams: TaskParams): ServiceConfig =
    new ServiceConfig(Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](serviceName, taskParams)))
}
