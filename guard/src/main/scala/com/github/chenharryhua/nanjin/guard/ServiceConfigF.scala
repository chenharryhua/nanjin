package com.github.chenharryhua.nanjin.guard

import cats.Applicative
import cats.derived.auto.functor._
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import retry.RetryPolicy

import scala.concurrent.duration._
import retry.RetryPolicies

sealed trait ServiceRetryPolicy {

  final def policy[F[_]](implicit F: Applicative[F]): RetryPolicy[F] = this match {
    case ConstantDelay(value)      => RetryPolicies.constantDelay(value)
    case ExponentialBackoff(value) => RetryPolicies.exponentialBackoff(value)
    case FibonacciBackoff(value)   => RetryPolicies.fibonacciBackoff(value)
    case FullJitter(value)         => RetryPolicies.fullJitter(value)
  }
  def value: FiniteDuration
}

final case class ConstantDelay(value: FiniteDuration) extends ServiceRetryPolicy
final case class ExponentialBackoff(value: FiniteDuration) extends ServiceRetryPolicy
final case class FibonacciBackoff(value: FiniteDuration) extends ServiceRetryPolicy
final case class FullJitter(value: FiniteDuration) extends ServiceRetryPolicy

@Lenses final case class ServiceParams private (
  applicationName: String,
  serviceName: String,
  healthCheckInterval: FiniteDuration,
  retryPolicy: ServiceRetryPolicy
)

private object ServiceParams {
  def apply(): ServiceParams = ServiceParams("unknown", "unknown", 6.hours, ConstantDelay(30.seconds))
}

sealed trait ServiceConfigF[F]

private object ServiceConfigF {

  final case class InitParams[K]() extends ServiceConfigF[K]
  final case class WithApplicationName[K](value: String, cont: K) extends ServiceConfigF[K]
  final case class WithServiceName[K](value: String, cont: K) extends ServiceConfigF[K]
  final case class WithHealthCheckInterval[K](value: FiniteDuration, cont: K) extends ServiceConfigF[K]
  final case class WithServiceDelayPolicy[K](value: ServiceRetryPolicy, cont: K) extends ServiceConfigF[K]

  val algebra: Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams()                  => ServiceParams()
      case WithApplicationName(v, c)     => ServiceParams.applicationName.set(v)(c)
      case WithServiceName(v, c)         => ServiceParams.serviceName.set(v)(c)
      case WithHealthCheckInterval(v, c) => ServiceParams.healthCheckInterval.set(v)(c)
      case WithServiceDelayPolicy(v, c)  => ServiceParams.retryPolicy.set(v)(c)
    }
}

final class ServiceConfig private (value: Fix[ServiceConfigF]) {
  import ServiceConfigF._

  def withApplicationName(appName: String): ServiceConfig = new ServiceConfig(Fix(WithApplicationName(appName, value)))
  def withServiceName(serviceName: String): ServiceConfig = new ServiceConfig(Fix(WithServiceName(serviceName, value)))

  def withHealthCheckInterval(d: FiniteDuration): ServiceConfig = new ServiceConfig(
    Fix(WithHealthCheckInterval(d, value)))

  def constantDelay(v: FiniteDuration): ServiceConfig =
    new ServiceConfig(Fix(WithServiceDelayPolicy(ConstantDelay(v), value)))

  def exponentialBackoff(v: FiniteDuration): ServiceConfig =
    new ServiceConfig(Fix(WithServiceDelayPolicy(ExponentialBackoff(v), value)))

  def fibonacciBackoff(v: FiniteDuration): ServiceConfig =
    new ServiceConfig(Fix(WithServiceDelayPolicy(FibonacciBackoff(v), value)))

  def fullJitter(v: FiniteDuration): ServiceConfig =
    new ServiceConfig(Fix(WithServiceDelayPolicy(FullJitter(v), value)))

  private[guard] def evalConfig: ServiceParams = scheme.cata(algebra).apply(value)
}

private object ServiceConfig {

  def default: ServiceConfig = new ServiceConfig(Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]]()))
}
