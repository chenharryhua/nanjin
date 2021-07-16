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
  retryPolicy: NJRetryPolicy,
  startUpEventDelay: FiniteDuration, // delay to sent out ServiceStarted event
  maxCauseSize: Int, // number of chars allowed to display in slack
  notes: String
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
      retryPolicy = ConstantDelay(30.seconds),
      startUpEventDelay = 15.seconds,
      maxCauseSize = 500,
      notes = ""
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

  final case class WithMaxCauseSize[K](value: Int, cont: K) extends ServiceConfigF[K]

  final case class WithNotes[K](value: String, cont: K) extends ServiceConfigF[K]

  val algebra: Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams(s, t)              => ServiceParams(s, t)
      case WithHealthCheckInterval(v, c) => ServiceParams.healthCheck.composeLens(NJHealthCheck.interval).set(v)(c)
      case WithHealthCheckOpenTime(v, c) => ServiceParams.healthCheck.composeLens(NJHealthCheck.openTime).set(v)(c)
      case WithHealthCheckSpan(v, c)     => ServiceParams.healthCheck.composeLens(NJHealthCheck.span).set(v)(c)
      case WithRetryPolicy(v, c)         => ServiceParams.retryPolicy.set(v)(c)
      case WithStartUpDelay(v, c)        => ServiceParams.startUpEventDelay.set(v)(c)
      case WithMaxCauseSize(v, c)        => ServiceParams.maxCauseSize.set(v)(c)
      case WithNotes(v, c)               => ServiceParams.notes.set(v)(c)
    }
}

final case class ServiceConfig private (value: Fix[ServiceConfigF]) {
  import ServiceConfigF._

  def health_check_interval(interval: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithHealthCheckInterval(interval, value)))

  def health_check_open_time(openTime: LocalTime): ServiceConfig =
    ServiceConfig(Fix(WithHealthCheckOpenTime(openTime, value)))

  def health_check_span(duration: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithHealthCheckSpan(duration, value)))

  def startup_delay(delay: FiniteDuration): ServiceConfig = ServiceConfig(Fix(WithStartUpDelay(delay, value)))
  def startup_notes(notes: String): ServiceConfig         = ServiceConfig(Fix(WithNotes(notes, value)))

  def constant_delay(delay: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithRetryPolicy(ConstantDelay(delay), value)))

  def jitter_backoff(maxDelay: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithRetryPolicy(JitterBackoff(maxDelay), value)))

  def maximum_cause_size(size: Int): ServiceConfig =
    ServiceConfig(Fix(WithMaxCauseSize(size, value)))

  def evalConfig: ServiceParams = scheme.cata(algebra).apply(value)
}

private[guard] object ServiceConfig {

  def apply(serviceName: String, taskParams: TaskParams): ServiceConfig = new ServiceConfig(
    Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](serviceName, taskParams)))
}
