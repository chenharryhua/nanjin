package com.github.chenharryhua.nanjin.guard.config

import cats.Functor
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses

import java.time.LocalTime
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/** allow disable sending out health-check event
  * @param interval:
  *   emit health-check event every interval
  * @param openTime:
  *   in duration of openTime + span, health-check event is allowed to be sent
  * @param span:
  *   event sending window
  */
@Lenses final case class NJHealthCheck private (interval: FiniteDuration, openTime: LocalTime, span: FiniteDuration)

@Lenses final case class ServiceParams private (
  serviceName: String,
  taskParams: TaskParams,
  healthCheck: NJHealthCheck,
  retry: NJRetryPolicy,
  startUpEventDelay: FiniteDuration, // delay to sent out ServiceStarted event
  brief: String,
  threshold: Severity // filter out events whose severity bigger than this one.
)

object ServiceParams {

  def apply(serviceName: String, taskParams: TaskParams): ServiceParams =
    ServiceParams(
      serviceName = serviceName,
      taskParams = taskParams,
      healthCheck = NJHealthCheck(
        6.hours, // at least one health-check will show-up in business hour
        LocalTime.of(7, 0), // business open
        FiniteDuration(24, TimeUnit.HOURS) // working hours
      ),
      retry = ConstantDelay(30.seconds),
      startUpEventDelay = 15.seconds,
      brief = "The developer is too lazy to provide a brief",
      threshold = Severity.Error
    )
}

sealed private[guard] trait ServiceConfigF[F]

private object ServiceConfigF {
  implicit val functorServiceConfigF: Functor[ServiceConfigF] = cats.derived.semiauto.functor[ServiceConfigF]

  final case class InitParams[K](serviceName: String, taskParams: TaskParams) extends ServiceConfigF[K]
  final case class WithHealthCheckInterval[K](value: FiniteDuration, cont: K) extends ServiceConfigF[K]
  final case class WithHealthCheckOpenTime[K](value: LocalTime, cont: K) extends ServiceConfigF[K]
  final case class WithHealthCheckSpan[K](value: FiniteDuration, cont: K) extends ServiceConfigF[K]

  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends ServiceConfigF[K]

  final case class WithStartUpDelay[K](value: FiniteDuration, cont: K) extends ServiceConfigF[K]

  final case class WithServiceBrief[K](value: String, cont: K) extends ServiceConfigF[K]

  final case class WithSeverityThreshold[K](value: Severity, cont: K) extends ServiceConfigF[K]

  val algebra: Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams(s, t)              => ServiceParams(s, t)
      case WithHealthCheckInterval(v, c) => ServiceParams.healthCheck.composeLens(NJHealthCheck.interval).set(v)(c)
      case WithHealthCheckOpenTime(v, c) => ServiceParams.healthCheck.composeLens(NJHealthCheck.openTime).set(v)(c)
      case WithHealthCheckSpan(v, c)     => ServiceParams.healthCheck.composeLens(NJHealthCheck.span).set(v)(c)
      case WithRetryPolicy(v, c)         => ServiceParams.retry.set(v)(c)
      case WithStartUpDelay(v, c)        => ServiceParams.startUpEventDelay.set(v)(c)
      case WithServiceBrief(v, c)        => ServiceParams.brief.set(v)(c)
      case WithSeverityThreshold(v, c)   => ServiceParams.threshold.set(v)(c)
    }
}

final case class ServiceConfig private (value: Fix[ServiceConfigF]) {
  import ServiceConfigF.*

  def withHealthCheckInterval(interval: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithHealthCheckInterval(interval, value)))

  def withHealthCheckOpenTime(openTime: LocalTime): ServiceConfig =
    ServiceConfig(Fix(WithHealthCheckOpenTime(openTime, value)))

  def withHealthCheckSpan(duration: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithHealthCheckSpan(duration, value)))

  def withStartupDelay(delay: FiniteDuration): ServiceConfig = ServiceConfig(Fix(WithStartUpDelay(delay, value)))
  def withBrief(notes: String): ServiceConfig                = ServiceConfig(Fix(WithServiceBrief(notes, value)))

  def withConstantDelay(delay: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithRetryPolicy(ConstantDelay(delay), value)))

  def withJitterBackoff(minDelay: FiniteDuration, maxDelay: FiniteDuration): ServiceConfig = {
    require(maxDelay > minDelay, s"maxDelay($maxDelay) should be strickly bigger than minDelay($minDelay)")
    ServiceConfig(Fix(WithRetryPolicy(JitterBackoff(minDelay, maxDelay), value)))
  }

  def withJitterBackoff(maxDelay: FiniteDuration): ServiceConfig =
    withJitterBackoff(FiniteDuration(0, TimeUnit.SECONDS), maxDelay)

  def withCritical: ServiceConfig = ServiceConfig(Fix(WithSeverityThreshold(Severity.Critical, value)))
  def withNotice: ServiceConfig   = ServiceConfig(Fix(WithSeverityThreshold(Severity.Notice, value)))

  def evalConfig: ServiceParams = scheme.cata(algebra).apply(value)
}

private[guard] object ServiceConfig {

  def apply(serviceName: String, taskParams: TaskParams): ServiceConfig = new ServiceConfig(
    Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](serviceName, taskParams)))
}
