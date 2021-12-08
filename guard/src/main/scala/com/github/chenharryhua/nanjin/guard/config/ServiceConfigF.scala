package com.github.chenharryhua.nanjin.guard.config

import cats.derived.auto.show.*
import cats.{Functor, Show}
import com.amazonaws.thirdparty.apache.codec.digest.DigestUtils
import com.github.chenharryhua.nanjin.datetime.instances.*
import cron4s.{Cron, CronExpr}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import monocle.macros.Lenses

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

@Lenses @JsonCodec final case class MetricParams(
  reportSchedule: Either[FiniteDuration, CronExpr],
  resetSchedule: Option[CronExpr],
  rateTimeUnit: TimeUnit,
  durationTimeUnit: TimeUnit
)

object MetricParams {
  implicit val showMetricParams: Show[MetricParams] = cats.derived.semiauto.show[MetricParams]
}

@Lenses @JsonCodec final case class ServiceParams(
  serviceName: String,
  taskParams: TaskParams,
  retry: NJRetryPolicy,
  queueCapacity: Int,
  metric: MetricParams
) {
  val sha1Hex: String    = DigestUtils.sha1Hex(s"${taskParams.appName}/${serviceName}").take(8)
  val uniqueName: String = s"$serviceName/$sha1Hex"
}

object ServiceParams {

  implicit val showServiceParams: Show[ServiceParams] = cats.derived.semiauto.show[ServiceParams]

  def apply(serviceName: String, taskParams: TaskParams): ServiceParams =
    ServiceParams(
      serviceName = serviceName,
      taskParams = taskParams,
      retry = NJRetryPolicy.ConstantDelay(30.seconds),
      queueCapacity = 0, // synchronous
      metric = MetricParams(
        reportSchedule = Left(1.hour),
        resetSchedule = None,
        rateTimeUnit = TimeUnit.SECONDS,
        durationTimeUnit = TimeUnit.MILLISECONDS
      )
    )
}

sealed private[guard] trait ServiceConfigF[F]

private object ServiceConfigF {
  implicit val functorServiceConfigF: Functor[ServiceConfigF] = cats.derived.semiauto.functor[ServiceConfigF]

  final case class InitParams[K](serviceName: String, taskParams: TaskParams) extends ServiceConfigF[K]
  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends ServiceConfigF[K]
  final case class WithServiceName[K](value: String, cont: K) extends ServiceConfigF[K]
  final case class WithQueueCapacity[K](value: Int, cont: K) extends ServiceConfigF[K]

  final case class WithReportSchedule[K](value: Either[FiniteDuration, CronExpr], cont: K) extends ServiceConfigF[K]
  final case class WithResetSchedule[K](value: Option[CronExpr], cont: K) extends ServiceConfigF[K]
  final case class WithRateTimeUnit[K](value: TimeUnit, cont: K) extends ServiceConfigF[K]
  final case class WithDurationTimeUnit[K](value: TimeUnit, cont: K) extends ServiceConfigF[K]

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
    }
}

final case class ServiceConfig private (value: Fix[ServiceConfigF]) {
  import ServiceConfigF.*

  def withQueueCapacity(size: Int): ServiceConfig  = ServiceConfig(Fix(WithQueueCapacity(size, value)))
  def withServiceName(name: String): ServiceConfig = ServiceConfig(Fix(WithServiceName(name, value)))

  def withMetricSchedule(interval: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithReportSchedule(Left(interval), value)))

  def withMetricSchedule(crontab: CronExpr): ServiceConfig =
    ServiceConfig(Fix(WithReportSchedule(Right(crontab), value)))

  def withMetricSchedule(crontab: String): ServiceConfig =
    withMetricSchedule(Cron.unsafeParse(crontab))

  def withMetricReset(crontab: CronExpr): ServiceConfig = ServiceConfig(Fix(WithResetSchedule(Some(crontab), value)))
  def withMetricReset(crontab: String): ServiceConfig   = withMetricReset(Cron.unsafeParse(crontab))
  def withMetricDailyReset: ServiceConfig               = withMetricReset(Cron.unsafeParse("1 0 0 ? * *"))
  def withMetricWeeklyReset: ServiceConfig              = withMetricReset(Cron.unsafeParse("1 0 0 ? * 0"))
  def withMetricMonthlyReset: ServiceConfig             = withMetricReset(Cron.unsafeParse("1 0 0 1 * ?"))

  def withMetricRateTimeUnit(tu: TimeUnit): ServiceConfig     = ServiceConfig(Fix(WithRateTimeUnit(tu, value)))
  def withMetricDurationTimeUnit(tu: TimeUnit): ServiceConfig = ServiceConfig(Fix(WithDurationTimeUnit(tu, value)))

  def withConstantDelay(delay: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithRetryPolicy(NJRetryPolicy.ConstantDelay(delay), value)))

  def withJitterBackoff(minDelay: FiniteDuration, maxDelay: FiniteDuration): ServiceConfig = {
    require(maxDelay > minDelay, s"maxDelay($maxDelay) should be strictly bigger than minDelay($minDelay)")
    ServiceConfig(Fix(WithRetryPolicy(NJRetryPolicy.JitterBackoff(minDelay, maxDelay), value)))
  }

  def withJitterBackoff(maxDelay: FiniteDuration): ServiceConfig =
    withJitterBackoff(FiniteDuration(0, TimeUnit.SECONDS), maxDelay)

  def evalConfig: ServiceParams = scheme.cata(algebra).apply(value)
}

private[guard] object ServiceConfig {

  def apply(serviceName: String, taskParams: TaskParams): ServiceConfig = new ServiceConfig(
    Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](serviceName, taskParams)))
}
