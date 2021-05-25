package com.github.chenharryhua.nanjin.guard

import cats.Applicative
import cats.derived.auto.functor._
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import retry.{RetryPolicies, RetryPolicy}

import scala.concurrent.duration._

sealed abstract class NJRetryPolicy {

  final def policy[F[_]](implicit F: Applicative[F]): RetryPolicy[F] = this match {
    case ConstantDelay(value)      => RetryPolicies.constantDelay(value)
    case ExponentialBackoff(value) => RetryPolicies.exponentialBackoff(value)
    case FibonacciBackoff(value)   => RetryPolicies.fibonacciBackoff(value)
    case FullJitter(value)         => RetryPolicies.fullJitter(value)
  }
  def value: FiniteDuration
}

final private case class ConstantDelay(value: FiniteDuration) extends NJRetryPolicy
final private case class ExponentialBackoff(value: FiniteDuration) extends NJRetryPolicy
final private case class FibonacciBackoff(value: FiniteDuration) extends NJRetryPolicy
final private case class FullJitter(value: FiniteDuration) extends NJRetryPolicy

@Lenses final case class HealthCheck(interval: FiniteDuration, isEnabled: Boolean)

@Lenses final case class ServiceParams private (
  healthCheck: HealthCheck,
  retryPolicy: NJRetryPolicy
)

private object ServiceParams {

  def apply(): ServiceParams =
    ServiceParams(healthCheck = HealthCheck(6.hours, isEnabled = true), retryPolicy = ConstantDelay(30.seconds))
}

sealed trait ServiceConfigF[F]

private object ServiceConfigF {

  final case class InitParams[K]() extends ServiceConfigF[K]
  final case class WithHealthCheckInterval[K](value: FiniteDuration, cont: K) extends ServiceConfigF[K]
  final case class WithHealthCheckFlag[K](value: Boolean, cont: K) extends ServiceConfigF[K]
  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends ServiceConfigF[K]

  val algebra: Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams()                  => ServiceParams()
      case WithHealthCheckInterval(v, c) => ServiceParams.healthCheck.composeLens(HealthCheck.interval).set(v)(c)
      case WithHealthCheckFlag(v, c)     => ServiceParams.healthCheck.composeLens(HealthCheck.isEnabled).set(v)(c)
      case WithRetryPolicy(v, c)         => ServiceParams.retryPolicy.set(v)(c)
    }
}

final case class ServiceConfig private (value: Fix[ServiceConfigF]) {
  import ServiceConfigF._

  def withHealthCheckInterval(d: FiniteDuration): ServiceConfig = ServiceConfig(Fix(WithHealthCheckInterval(d, value)))
  def withHealthCheckDisabled: ServiceConfig                    = ServiceConfig(Fix(WithHealthCheckFlag(value = false, value)))

  def withConstantDelay(v: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithRetryPolicy(ConstantDelay(v), value)))

  def withExponentialBackoff(v: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithRetryPolicy(ExponentialBackoff(v), value)))

  def withFibonacciBackoff(v: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithRetryPolicy(FibonacciBackoff(v), value)))

  def withFullJitter(v: FiniteDuration): ServiceConfig =
    ServiceConfig(Fix(WithRetryPolicy(FullJitter(v), value)))

  def evalConfig: ServiceParams = scheme.cata(algebra).apply(value)
}

private object ServiceConfig {

  def default: ServiceConfig = new ServiceConfig(Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]]()))
}
