package com.github.chenharryhua.nanjin.guard

import cats.derived.auto.functor._
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses

import scala.concurrent.duration._

@Lenses final case class HealthCheck(interval: FiniteDuration, isEnabled: Boolean)

@Lenses final case class ServiceParams private (
  healthCheck: HealthCheck,
  retryPolicy: NJRetryPolicy,
  topicMaxQueued: Int, // for fs2 topic
  startUpEventDelay: FiniteDuration, // delay to sent out ServiceStarted event
  isLogging: Boolean // enable logging
)

private object ServiceParams {

  def apply(): ServiceParams =
    ServiceParams(
      healthCheck = HealthCheck(6.hours, isEnabled = true),
      retryPolicy = ConstantDelay(30.seconds),
      topicMaxQueued = 10,
      startUpEventDelay = 15.seconds,
      isLogging = true
    )
}

sealed trait ServiceConfigF[F]

private object ServiceConfigF {

  final case class InitParams[K]() extends ServiceConfigF[K]
  final case class WithHealthCheckInterval[K](value: FiniteDuration, cont: K) extends ServiceConfigF[K]
  final case class WithHealthCheckFlag[K](value: Boolean, cont: K) extends ServiceConfigF[K]
  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends ServiceConfigF[K]

  final case class WithTopicMaxQueued[K](value: Int, cont: K) extends ServiceConfigF[K]

  final case class WithStartUpDelay[K](value: FiniteDuration, cont: K) extends ServiceConfigF[K]

  final case class WithLoggingEnabled[K](value: Boolean, cont: K) extends ServiceConfigF[K]

  val algebra: Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams()                  => ServiceParams()
      case WithHealthCheckInterval(v, c) => ServiceParams.healthCheck.composeLens(HealthCheck.interval).set(v)(c)
      case WithHealthCheckFlag(v, c)     => ServiceParams.healthCheck.composeLens(HealthCheck.isEnabled).set(v)(c)
      case WithRetryPolicy(v, c)         => ServiceParams.retryPolicy.set(v)(c)
      case WithTopicMaxQueued(v, c)      => ServiceParams.topicMaxQueued.set(v)(c)
      case WithStartUpDelay(v, c)        => ServiceParams.startUpEventDelay.set(v)(c)
      case WithLoggingEnabled(v, c)      => ServiceParams.isLogging.set(v)(c)
    }
}

final case class ServiceConfig private (value: Fix[ServiceConfigF]) {
  import ServiceConfigF._

  def withHealthCheckInterval(interval: FiniteDuration): ServiceConfig = ServiceConfig(
    Fix(WithHealthCheckInterval(interval, value)))
  def withHealthCheckDisabled: ServiceConfig = ServiceConfig(Fix(WithHealthCheckFlag(value = false, value)))

  def withTopicMaxQueued(num: Int): ServiceConfig = ServiceConfig(Fix(WithTopicMaxQueued(num, value)))

  def withStartUpDelay(delay: FiniteDuration): ServiceConfig = ServiceConfig(Fix(WithStartUpDelay(delay, value)))

  def withLoggingDisabled: ServiceConfig = ServiceConfig(Fix(WithLoggingEnabled(value = false, value)))

  def withConstantDelay(delay: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithRetryPolicy(ConstantDelay(delay), value)))

  def evalConfig: ServiceParams = scheme.cata(algebra).apply(value)
}

private object ServiceConfig {

  def default: ServiceConfig = new ServiceConfig(Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]]()))
}
