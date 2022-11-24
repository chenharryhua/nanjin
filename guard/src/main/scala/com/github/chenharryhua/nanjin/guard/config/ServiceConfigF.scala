package com.github.chenharryhua.nanjin.guard.config

import cats.{Functor, Show}
import cats.effect.kernel.Clock
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.guard.ServiceName
import cron4s.{Cron, CronExpr}
import cron4s.lib.javatime.javaTemporalInstance
import eu.timepit.refined.cats.*
import higherkindness.droste.{scheme, Algebra}
import higherkindness.droste.data.Fix
import io.circe.{Decoder, Encoder, Json}
import io.circe.generic.JsonCodec
import io.circe.refined.*
import io.scalaland.enumz.Enum
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
  rateTimeUnit: TimeUnit,
  durationTimeUnit: TimeUnit) {
  def nextReport(now: ZonedDateTime): Option[ZonedDateTime] = reportSchedule.flatMap(_.next(now))
  def nextReset(now: ZonedDateTime): Option[ZonedDateTime]  = resetSchedule.flatMap(_.next(now))
}

object MetricParams {
  private[this] val enumTimeUnit: Enum[TimeUnit]        = Enum[TimeUnit]
  implicit final val encoderTimeUnit: Encoder[TimeUnit] = Encoder.encodeString.contramap(enumTimeUnit.getName)
  implicit final val decoderTimeUnit: Decoder[TimeUnit] = Decoder.decodeString.map(enumTimeUnit.withName)
  implicit final val showTimeUnit: Show[TimeUnit]       = enumTimeUnit.getName

  implicit val showMetricParams: Show[MetricParams] = cats.derived.semiauto.show[MetricParams]
}

@Lenses @JsonCodec final case class ServiceParams(
  serviceName: ServiceName,
  serviceId: UUID,
  launchTime: ZonedDateTime,
  retryPolicy: String, // service restart policy
  policyThreshold: Option[Duration], // policy start over interval
  taskParams: TaskParams,
  metricParams: MetricParams,
  brief: Json
) {
  def toZonedDateTime(ts: Instant): ZonedDateTime = ts.atZone(taskParams.zoneId)
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
    retryPolicy: String, // for display
    brief: Json
  ): ServiceParams =
    ServiceParams(
      serviceName = serviceName,
      serviceId = serviceId,
      launchTime = launchTime.atZone(taskParams.zoneId),
      taskParams = taskParams,
      retryPolicy = retryPolicy,
      policyThreshold = None,
      metricParams = MetricParams(
        reportSchedule = None,
        resetSchedule = None,
        rateTimeUnit = TimeUnit.SECONDS,
        durationTimeUnit = TimeUnit.MILLISECONDS
      ),
      brief = brief
    )
}

sealed private[guard] trait ServiceConfigF[X]

private object ServiceConfigF {
  implicit val functorServiceConfigF: Functor[ServiceConfigF] = cats.derived.semiauto.functor[ServiceConfigF]

  final case class InitParams[K]() extends ServiceConfigF[K]

  final case class WithReportSchedule[K](value: Option[CronExpr], cont: K) extends ServiceConfigF[K]
  final case class WithResetSchedule[K](value: Option[CronExpr], cont: K) extends ServiceConfigF[K]
  final case class WithRateTimeUnit[K](value: TimeUnit, cont: K) extends ServiceConfigF[K]
  final case class WithDurationTimeUnit[K](value: TimeUnit, cont: K) extends ServiceConfigF[K]

  final case class WithPolicyThreshold[K](value: Option[Duration], cont: K) extends ServiceConfigF[K]

  def algebra(
    serviceName: ServiceName,
    taskParams: TaskParams,
    serviceId: UUID,
    launchTime: Instant,
    retryPolicy: String,
    brief: Json): Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams() => ServiceParams(serviceName, taskParams, serviceId, launchTime, retryPolicy, brief)

      case WithReportSchedule(v, c) =>
        ServiceParams.metricParams.composeLens(MetricParams.reportSchedule).set(v)(c)
      case WithResetSchedule(v, c) =>
        ServiceParams.metricParams.composeLens(MetricParams.resetSchedule).set(v)(c)
      case WithRateTimeUnit(v, c) =>
        ServiceParams.metricParams.composeLens(MetricParams.rateTimeUnit).set(v)(c)
      case WithDurationTimeUnit(v, c) =>
        ServiceParams.metricParams.composeLens(MetricParams.durationTimeUnit).set(v)(c)

      case WithPolicyThreshold(v, c) => ServiceParams.policyThreshold.set(v)(c)
    }
}

final case class ServiceConfig private (value: Fix[ServiceConfigF], taskParams: TaskParams) {
  import ServiceConfigF.*

  // metrics
  def withMetricReport(crontab: CronExpr): ServiceConfig =
    ServiceConfig(Fix(WithReportSchedule(Some(crontab), value)), taskParams)

  def withMetricReport(crontab: String): ServiceConfig =
    withMetricReport(Cron.unsafeParse(crontab))

  def withMetricReset(crontab: CronExpr): ServiceConfig =
    ServiceConfig(Fix(WithResetSchedule(Some(crontab), value)), taskParams)
  def withMetricReset(crontab: String): ServiceConfig = withMetricReset(Cron.unsafeParse(crontab))
  def withMetricDailyReset: ServiceConfig             = withMetricReset(dailyCron)
  def withMetricWeeklyReset: ServiceConfig            = withMetricReset(weeklyCron)
  def withMetricMonthlyReset: ServiceConfig           = withMetricReset(monthlyCron)

  def withoutMetricReport: ServiceConfig = ServiceConfig(Fix(WithReportSchedule(None, value)), taskParams)
  def withoutMetricReset: ServiceConfig  = ServiceConfig(Fix(WithResetSchedule(None, value)), taskParams)

  def withMetricRateTimeUnit(tu: TimeUnit): ServiceConfig =
    ServiceConfig(Fix(WithRateTimeUnit(tu, value)), taskParams)
  def withMetricDurationTimeUnit(tu: TimeUnit): ServiceConfig =
    ServiceConfig(Fix(WithDurationTimeUnit(tu, value)), taskParams)

  def withPolicyThreshold(fd: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithPolicyThreshold(Some(fd.toJava), value)), taskParams)

  def evalConfig(
    serviceName: ServiceName,
    serviceId: UUID,
    launchTime: Instant,
    retryPolicy: String,
    brief: Json): ServiceParams =
    scheme.cata(algebra(serviceName, taskParams, serviceId, launchTime, retryPolicy, brief)).apply(value)
}

private[guard] object ServiceConfig {

  def apply(taskParams: TaskParams): ServiceConfig =
    new ServiceConfig(Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]]()), taskParams)
}
