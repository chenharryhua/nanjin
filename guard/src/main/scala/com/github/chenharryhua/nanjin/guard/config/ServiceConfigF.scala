package com.github.chenharryhua.nanjin.guard.config

import cats.effect.kernel.Clock
import cats.implicits.toFunctorOps
import cats.{Functor, Show}
import cron4s.lib.javatime.javaTemporalInstance
import cron4s.{Cron, CronExpr}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.Json
import io.circe.generic.JsonCodec
import monocle.macros.Lenses
import org.typelevel.cats.time.instances.{duration, zoneddatetime}

import java.time.*
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

@Lenses @JsonCodec final case class MetricParams(
  reportSchedule: Option[CronExpr],
  resetSchedule: Option[CronExpr],
  namePrefix: String,
  rateTimeUnit: TimeUnit,
  durationTimeUnit: TimeUnit) {
  def nextReport(now: ZonedDateTime): Option[ZonedDateTime] = reportSchedule.flatMap(_.next(now))
  def nextReset(now: ZonedDateTime): Option[ZonedDateTime]  = resetSchedule.flatMap(_.next(now))

  val rateUnitName: String = rateTimeUnit.name().toLowerCase.dropRight(1)

  // dropwizard default is times / second
  def rateConversion(rate: Double): Double = rate * rateTimeUnit.toSeconds(1)
}

object MetricParams {
  implicit val showMetricParams: Show[MetricParams] = cats.derived.semiauto.show[MetricParams]
}

@Lenses @JsonCodec final case class ServiceParams(
  serviceName: String,
  serviceId: UUID,
  launchTime: ZonedDateTime,
  restartPolicy: String, // for display
  policyThreshold: Option[Duration], // policy start over interval
  taskParams: TaskParams,
  metricParams: MetricParams,
  homePage: Option[String],
  brief: Json
) {
  def toZonedDateTime(ts: Instant): ZonedDateTime = ts.atZone(taskParams.zoneId)
  def toZonedDateTime(fd: FiniteDuration): ZonedDateTime =
    toZonedDateTime(Instant.EPOCH.plusNanos(fd.toNanos))

  def toLocalDateTime(ts: Instant): LocalDateTime = toZonedDateTime(ts).toLocalDateTime
  def toLocalDate(ts: Instant): LocalDate         = toZonedDateTime(ts).toLocalDate
  def toLocalTime(ts: Instant): LocalTime         = toZonedDateTime(ts).toLocalTime
  def upTime(ts: ZonedDateTime): Duration         = Duration.between(launchTime, ts)
  def upTime(ts: Instant): Duration               = Duration.between(launchTime, toZonedDateTime(ts))

  def zonedNow[F[_]: Clock: Functor]: F[ZonedDateTime] = Clock[F].realTimeInstant.map(toZonedDateTime)
}

object ServiceParams extends zoneddatetime with duration {

  implicit val showServiceParams: Show[ServiceParams] = cats.derived.semiauto.show[ServiceParams]

  def apply(
    serviceName: ServiceName,
    taskParams: TaskParams,
    serviceId: UUID,
    launchTime: Instant,
    policy: Policy, // for display
    brief: Json
  ): ServiceParams =
    ServiceParams(
      serviceName = serviceName.value,
      serviceId = serviceId,
      launchTime = launchTime.atZone(taskParams.zoneId),
      taskParams = taskParams,
      restartPolicy = policy.value,
      policyThreshold = None,
      metricParams = MetricParams(
        reportSchedule = None,
        resetSchedule = None,
        namePrefix = "",
        rateTimeUnit = TimeUnit.SECONDS,
        durationTimeUnit = TimeUnit.MILLISECONDS
      ),
      homePage = None,
      brief = brief
    )
}

sealed private[guard] trait ServiceConfigF[X]

private object ServiceConfigF {
  implicit val functorServiceConfigF: Functor[ServiceConfigF] = cats.derived.semiauto.functor[ServiceConfigF]

  final case class InitParams[K](taskParams: TaskParams) extends ServiceConfigF[K]

  final case class WithReportSchedule[K](value: Option[CronExpr], cont: K) extends ServiceConfigF[K]
  final case class WithResetSchedule[K](value: Option[CronExpr], cont: K) extends ServiceConfigF[K]
  final case class WithRateTimeUnit[K](value: TimeUnit, cont: K) extends ServiceConfigF[K]
  final case class WithDurationTimeUnit[K](value: TimeUnit, cont: K) extends ServiceConfigF[K]
  final case class WithMetricNamePrefix[K](value: String, cont: K) extends ServiceConfigF[K]

  final case class WithPolicyThreshold[K](value: Option[Duration], cont: K) extends ServiceConfigF[K]

  final case class WithHomePage[K](value: Option[String], cont: K) extends ServiceConfigF[K]

  def algebra(
    serviceName: ServiceName,
    serviceId: UUID,
    launchTime: Instant,
    retryPolicy: Policy,
    brief: Json): Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams(taskParams) =>
        ServiceParams(serviceName, taskParams, serviceId, launchTime, retryPolicy, brief)

      case WithReportSchedule(v, c) =>
        ServiceParams.metricParams.composeLens(MetricParams.reportSchedule).set(v)(c)
      case WithResetSchedule(v, c) =>
        ServiceParams.metricParams.composeLens(MetricParams.resetSchedule).set(v)(c)
      case WithRateTimeUnit(v, c) =>
        ServiceParams.metricParams.composeLens(MetricParams.rateTimeUnit).set(v)(c)
      case WithDurationTimeUnit(v, c) =>
        ServiceParams.metricParams.composeLens(MetricParams.durationTimeUnit).set(v)(c)
      case WithMetricNamePrefix(v, c) =>
        ServiceParams.metricParams.composeLens(MetricParams.namePrefix).set(v)(c)

      case WithPolicyThreshold(v, c) => ServiceParams.policyThreshold.set(v)(c)

      case WithHomePage(v, c) => ServiceParams.homePage.set(v)(c)
    }
}

final case class ServiceConfig private (private val cont: Fix[ServiceConfigF]) {
  import ServiceConfigF.*

  // metrics
  def withMetricReport(crontab: CronExpr): ServiceConfig =
    ServiceConfig(Fix(WithReportSchedule(Some(crontab), cont)))

  def withMetricReport(crontab: String): ServiceConfig =
    withMetricReport(Cron.unsafeParse(crontab))

  def withMetricReset(crontab: CronExpr): ServiceConfig =
    ServiceConfig(Fix(WithResetSchedule(Some(crontab), cont)))
  def withMetricReset(crontab: String): ServiceConfig = withMetricReset(Cron.unsafeParse(crontab))
  def withMetricDailyReset: ServiceConfig             = withMetricReset(dailyCron)
  def withMetricWeeklyReset: ServiceConfig            = withMetricReset(weeklyCron)
  def withMetricMonthlyReset: ServiceConfig           = withMetricReset(monthlyCron)

  def withoutMetricReport: ServiceConfig = ServiceConfig(Fix(WithReportSchedule(None, cont)))
  def withoutMetricReset: ServiceConfig  = ServiceConfig(Fix(WithResetSchedule(None, cont)))

  def withMetricRateTimeUnit(tu: TimeUnit): ServiceConfig =
    ServiceConfig(Fix(WithRateTimeUnit(tu, cont)))
  def withMetricDurationTimeUnit(tu: TimeUnit): ServiceConfig =
    ServiceConfig(Fix(WithDurationTimeUnit(tu, cont)))

  def withMetricNamePrefix(prefix: String): ServiceConfig =
    ServiceConfig(Fix(WithMetricNamePrefix(prefix, cont)))

  def withPolicyThreshold(fd: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithPolicyThreshold(Some(fd.toJava), cont)))

  def withHomePage(hp: String): ServiceConfig =
    ServiceConfig(Fix(WithHomePage(Some(hp), cont)))

  def evalConfig(
    serviceName: ServiceName,
    serviceId: UUID,
    launchTime: Instant,
    policy: Policy,
    brief: Json): ServiceParams =
    scheme.cata(algebra(serviceName, serviceId, launchTime, policy, brief)).apply(cont)
}

private[guard] object ServiceConfig {

  def apply(taskParams: TaskParams): ServiceConfig =
    new ServiceConfig(Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](taskParams)))
}
