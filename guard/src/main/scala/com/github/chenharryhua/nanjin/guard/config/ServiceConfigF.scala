package com.github.chenharryhua.nanjin.guard.config

import cats.Functor
import cron4s.{Cron, CronExpr}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

@Lenses final case class ServiceParams private (
  serviceName: String,
  taskParams: TaskParams,
  retry: NJRetryPolicy,
  reportingSchedule: Either[FiniteDuration, CronExpr],
  brief: String
)

object ServiceParams {

  def apply(serviceName: String, taskParams: TaskParams): ServiceParams =
    ServiceParams(
      serviceName = serviceName,
      taskParams = taskParams,
      retry = NJRetryPolicy.ConstantDelay(30.seconds),
      reportingSchedule = Left(1.hour),
      brief = "The developer is too lazy to provide a brief"
    )
}

sealed private[guard] trait ServiceConfigF[F]

private object ServiceConfigF {
  implicit val functorServiceConfigF: Functor[ServiceConfigF] = cats.derived.semiauto.functor[ServiceConfigF]

  final case class InitParams[K](serviceName: String, taskParams: TaskParams) extends ServiceConfigF[K]
  final case class WithReportingSchedule[K](value: Either[FiniteDuration, CronExpr], cont: K) extends ServiceConfigF[K]

  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends ServiceConfigF[K]

  final case class WithServiceBrief[K](value: String, cont: K) extends ServiceConfigF[K]

  final case class WithServiceName[K](value: String, cont: K) extends ServiceConfigF[K]

  val algebra: Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams(s, t)            => ServiceParams(s, t)
      case WithRetryPolicy(v, c)       => ServiceParams.retry.set(v)(c)
      case WithServiceBrief(v, c)      => ServiceParams.brief.set(v)(c)
      case WithReportingSchedule(v, c) => ServiceParams.reportingSchedule.set(v)(c)
      case WithServiceName(v, c)       => ServiceParams.serviceName.set(v)(c)
    }
}

final case class ServiceConfig private (value: Fix[ServiceConfigF]) {
  import ServiceConfigF.*

  def withServiceName(name: String): ServiceConfig =
    ServiceConfig(Fix(WithServiceName(name, value)))

  def withReportingSchedule(interval: FiniteDuration): ServiceConfig = ServiceConfig(
    Fix(WithReportingSchedule(Left(interval), value)))

  def withReportingSchedule(crontab: CronExpr): ServiceConfig =
    ServiceConfig(Fix(WithReportingSchedule(Right(crontab), value)))

  def withReportingSchedule(crontab: String): ServiceConfig =
    withReportingSchedule(Cron.unsafeParse(crontab))

  def withBrief(notes: String): ServiceConfig = ServiceConfig(Fix(WithServiceBrief(notes, value)))

  def withConstantDelay(delay: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithRetryPolicy(NJRetryPolicy.ConstantDelay(delay), value)))

  def withJitterBackoff(minDelay: FiniteDuration, maxDelay: FiniteDuration): ServiceConfig = {
    require(maxDelay > minDelay, s"maxDelay($maxDelay) should be strickly bigger than minDelay($minDelay)")
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
