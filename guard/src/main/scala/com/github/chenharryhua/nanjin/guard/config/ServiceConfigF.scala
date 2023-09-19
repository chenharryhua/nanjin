package com.github.chenharryhua.nanjin.guard.config

import cats.effect.kernel.Clock
import cats.implicits.toFunctorOps
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.chrono.Tick
import cron4s.{Cron, CronExpr}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.Json
import io.circe.generic.JsonCodec
import monocle.syntax.all.*
import org.typelevel.cats.time.instances.{duration, zoneddatetime}

import java.time.*
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

@JsonCodec
final case class MetricParams(
  reportSchedule: Option[CronExpr],
  resetSchedule: Option[CronExpr],
  namePrefix: String,
  rateTimeUnit: TimeUnit,
  durationTimeUnit: TimeUnit) {

  val rateUnitName: String = rateTimeUnit.name().toLowerCase.dropRight(1)

  // dropwizard default is times / second
  def rateConversion(rate: Double): Double = rate * rateTimeUnit.toSeconds(1)
}

object MetricParams {
  implicit val showMetricParams: Show[MetricParams] = cats.derived.semiauto.show[MetricParams]
}

@JsonCodec
final case class ServiceParams(
  serviceName: String,
  restartPolicy: String, // for display
  taskParams: TaskParams,
  metricParams: MetricParams,
  homePage: Option[String],
  brief: Option[Json],
  zeroth: Tick
) {
  val serviceId: UUID           = zeroth.sequenceId
  val launchTime: ZonedDateTime = zeroth.launchTime.atZone(taskParams.zoneId)

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
    policy: ServicePolicy, // for display
    brief: ServiceBrief,
    zeroth: Tick
  ): ServiceParams =
    ServiceParams(
      serviceName = serviceName.value,
      taskParams = taskParams,
      restartPolicy = policy.value,
      metricParams = MetricParams(
        reportSchedule = None,
        resetSchedule = None,
        namePrefix = "",
        rateTimeUnit = TimeUnit.SECONDS,
        durationTimeUnit = TimeUnit.MILLISECONDS
      ),
      homePage = None,
      brief = brief.value,
      zeroth = zeroth
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

  final case class WithHomePage[K](value: Option[String], cont: K) extends ServiceConfigF[K]

  def algebra(
    serviceName: ServiceName,
    retryPolicy: ServicePolicy,
    brief: ServiceBrief,
    groundZero: Tick): Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams(taskParams) =>
        ServiceParams(serviceName, taskParams, retryPolicy, brief, groundZero)

      case WithReportSchedule(v, c)   => c.focus(_.metricParams.reportSchedule).replace(v)
      case WithResetSchedule(v, c)    => c.focus(_.metricParams.resetSchedule).replace(v)
      case WithRateTimeUnit(v, c)     => c.focus(_.metricParams.rateTimeUnit).replace(v)
      case WithDurationTimeUnit(v, c) => c.focus(_.metricParams.durationTimeUnit).replace(v)
      case WithMetricNamePrefix(v, c) => c.focus(_.metricParams.namePrefix).replace(v)
      case WithHomePage(v, c)         => c.focus(_.homePage).replace(v)
    }
}

final case class ServiceConfig(cont: Fix[ServiceConfigF]) extends AnyVal {
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

  def withHomePage(hp: String): ServiceConfig =
    ServiceConfig(Fix(WithHomePage(Some(hp), cont)))

  def evalConfig(
    serviceName: ServiceName,
    policy: ServicePolicy,
    brief: ServiceBrief,
    zeroth: Tick): ServiceParams =
    scheme.cata(algebra(serviceName, policy, brief, zeroth)).apply(cont)
}

object ServiceConfig {

  def apply(taskParams: TaskParams): ServiceConfig =
    new ServiceConfig(Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](taskParams)))
}
