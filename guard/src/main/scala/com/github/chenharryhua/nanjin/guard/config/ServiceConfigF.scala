package com.github.chenharryhua.nanjin.guard.config

import cats.{Functor, Show}
import cats.effect.kernel.Clock
import cats.implicits.toFunctorOps
import com.github.chenharryhua.nanjin.common.guard.{QueueCapacity, ServiceName}
import cron4s.{Cron, CronExpr}
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
import org.typelevel.cats.time.instances.{duration, zoneddatetime}

import java.time.*
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

@Lenses @JsonCodec final case class MetricParams(
  reportSchedule: Option[ScheduleType],
  resetSchedule: Option[CronExpr],
  rateTimeUnit: TimeUnit,
  durationTimeUnit: TimeUnit) {
  def nextReport(now: ZonedDateTime): Option[ZonedDateTime] =
    reportSchedule.flatMap(_.fold(fd => Some(now.plus(fd)), _.next(now)))
  def nextReset(now: ZonedDateTime): Option[ZonedDateTime] =
    resetSchedule.flatMap(_.next(now))
}

object MetricParams {
  private[this] val enumTimeUnit: Enum[TimeUnit]        = Enum[TimeUnit]
  implicit final val encoderTimeUnit: Encoder[TimeUnit] = Encoder.encodeString.contramap(enumTimeUnit.getName)
  implicit final val decoderTimeUnit: Decoder[TimeUnit] = Decoder.decodeString.map(enumTimeUnit.withName)
  implicit final val showTimeUnit: Show[TimeUnit]       = enumTimeUnit.getName

  implicit val showMetricParams: Show[MetricParams] = cats.derived.semiauto.show[MetricParams]
}

@Lenses @JsonCodec final case class ServiceParams private (
  serviceName: ServiceName,
  taskParams: TaskParams,
  retryPolicy: String, // service restart policy
  policyThreshold: Option[Duration], // policy start over interval
  queueCapacity: QueueCapacity, // event queue capacity
  metric: MetricParams,
  brief: String,
  serviceId: UUID,
  launchTime: ZonedDateTime
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
    retryPolicy: String // for display
  ): ServiceParams =
    ServiceParams(
      serviceName = serviceName,
      taskParams = taskParams,
      retryPolicy = retryPolicy,
      policyThreshold = None,
      queueCapacity = refineMV(0), // synchronous
      metric = MetricParams(
        reportSchedule = None,
        resetSchedule = None,
        rateTimeUnit = TimeUnit.SECONDS,
        durationTimeUnit = TimeUnit.MILLISECONDS
      ),
      brief = "no brief",
      serviceId = serviceId,
      launchTime = launchTime.atZone(taskParams.zoneId)
    )
}

sealed private[guard] trait ServiceConfigF[X]

private object ServiceConfigF {
  implicit val functorServiceConfigF: Functor[ServiceConfigF] = cats.derived.semiauto.functor[ServiceConfigF]

  final case class InitParams[K](serviceName: ServiceName, taskParams: TaskParams) extends ServiceConfigF[K]
  final case class WithServiceName[K](value: ServiceName, cont: K) extends ServiceConfigF[K]
  final case class WithQueueCapacity[K](value: QueueCapacity, cont: K) extends ServiceConfigF[K]

  final case class WithReportSchedule[K](value: Option[ScheduleType], cont: K) extends ServiceConfigF[K]
  final case class WithResetSchedule[K](value: Option[CronExpr], cont: K) extends ServiceConfigF[K]
  final case class WithRateTimeUnit[K](value: TimeUnit, cont: K) extends ServiceConfigF[K]
  final case class WithDurationTimeUnit[K](value: TimeUnit, cont: K) extends ServiceConfigF[K]

  final case class WithPolicyThreshold[K](value: Option[Duration], cont: K) extends ServiceConfigF[K]

  final case class WithBrief[K](value: String, cont: K) extends ServiceConfigF[K]

  def algebra(
    serviceId: UUID,
    launchTime: Instant,
    retryPolicy: String): Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams(s, t)        => ServiceParams(s, t, serviceId, launchTime, retryPolicy)
      case WithServiceName(v, c)   => ServiceParams.serviceName.set(v)(c)
      case WithQueueCapacity(v, c) => ServiceParams.queueCapacity.set(v)(c)

      case WithReportSchedule(v, c) => ServiceParams.metric.composeLens(MetricParams.reportSchedule).set(v)(c)
      case WithResetSchedule(v, c)  => ServiceParams.metric.composeLens(MetricParams.resetSchedule).set(v)(c)
      case WithRateTimeUnit(v, c)   => ServiceParams.metric.composeLens(MetricParams.rateTimeUnit).set(v)(c)
      case WithDurationTimeUnit(v, c) =>
        ServiceParams.metric.composeLens(MetricParams.durationTimeUnit).set(v)(c)

      case WithPolicyThreshold(v, c) => ServiceParams.policyThreshold.set(v)(c)

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

  def withoutMetricReport: ServiceConfig = ServiceConfig(Fix(WithReportSchedule(None, value)))
  def withoutMetricReset: ServiceConfig  = ServiceConfig(Fix(WithResetSchedule(None, value)))

  def withMetricRateTimeUnit(tu: TimeUnit): ServiceConfig = ServiceConfig(Fix(WithRateTimeUnit(tu, value)))
  def withMetricDurationTimeUnit(tu: TimeUnit): ServiceConfig =
    ServiceConfig(Fix(WithDurationTimeUnit(tu, value)))

  def withPolicyThreshold(dur: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithPolicyThreshold(Some(dur.toJava), value)))

  def evalConfig(serviceId: UUID, launchTime: Instant, retryPolicy: String): ServiceParams =
    scheme.cata(algebra(serviceId, launchTime, retryPolicy)).apply(value)
}

private[guard] object ServiceConfig {

  def apply(serviceName: ServiceName, taskParams: TaskParams): ServiceConfig =
    new ServiceConfig(Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](serviceName, taskParams)))
}
