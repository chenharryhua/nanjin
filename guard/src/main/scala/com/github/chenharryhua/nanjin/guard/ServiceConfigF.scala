package com.github.chenharryhua.nanjin.guard

import cats.Applicative
import cats.derived.auto.functor._
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import retry.{RetryPolicies, RetryPolicy}

import scala.concurrent.duration._

sealed trait NJRetryPolicy {

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

@Lenses final case class ServiceParams private (
  applicationName: String,
  serviceName: String,
  healthCheckInterval: FiniteDuration,
  retryPolicy: NJRetryPolicy
)

private object ServiceParams {

  def apply(): ServiceParams =
    ServiceParams(
      applicationName = "unknown",
      serviceName = "unknown",
      healthCheckInterval = 6.hours,
      retryPolicy = ConstantDelay(30.seconds))
}

sealed trait ServiceConfigF[F]

private object ServiceConfigF {

  final case class InitParams[K]() extends ServiceConfigF[K]
  final case class WithApplicationName[K](value: String, cont: K) extends ServiceConfigF[K]
  final case class WithServiceName[K](value: String, cont: K) extends ServiceConfigF[K]
  final case class WithHealthCheckInterval[K](value: FiniteDuration, cont: K) extends ServiceConfigF[K]
  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends ServiceConfigF[K]

  val algebra: Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams()                  => ServiceParams()
      case WithApplicationName(v, c)     => ServiceParams.applicationName.set(v)(c)
      case WithServiceName(v, c)         => ServiceParams.serviceName.set(v)(c)
      case WithHealthCheckInterval(v, c) => ServiceParams.healthCheckInterval.set(v)(c)
      case WithRetryPolicy(v, c)         => ServiceParams.retryPolicy.set(v)(c)
    }
}

final case class ServiceConfig private (value: Fix[ServiceConfigF]) {
  import ServiceConfigF._

  def withApplicationName(appName: String): ServiceConfig = ServiceConfig(Fix(WithApplicationName(appName, value)))
  def withServiceName(serviceName: String): ServiceConfig = ServiceConfig(Fix(WithServiceName(serviceName, value)))

  def withHealthCheckInterval(d: FiniteDuration): ServiceConfig = ServiceConfig(Fix(WithHealthCheckInterval(d, value)))

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
