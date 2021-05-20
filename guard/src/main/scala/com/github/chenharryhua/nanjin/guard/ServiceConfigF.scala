package com.github.chenharryhua.nanjin.guard

import cats.derived.auto.functor._
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses

import scala.concurrent.duration._

final case class ApplicationName(value: String) extends AnyVal
final case class ServiceName(value: String) extends AnyVal

final case class RestartInterval(value: FiniteDuration) extends AnyVal
final case class HealthCheckInterval(value: FiniteDuration) extends AnyVal

@Lenses final case class ServiceParams(
  applicationName: ApplicationName,
  serviceName: ServiceName,
  restartInterval: RestartInterval,
  healthCheckInterval: HealthCheckInterval
)

private object ServiceParams {

  def apply(appName: String): ServiceParams = ServiceParams(
    ApplicationName(appName),
    ServiceName("unknown"),
    RestartInterval(30.seconds),
    HealthCheckInterval(6.hours)
  )
}

sealed trait ServiceConfigF[F]

object ServiceConfigF {

  final case class InitParams[K](value: String) extends ServiceConfigF[K]
  final case class WithServiceName[K](value: String, cont: K) extends ServiceConfigF[K]
  final case class WithRestartInterval[K](value: FiniteDuration, cont: K) extends ServiceConfigF[K]
  final case class WithHealthCheckInterval[K](value: FiniteDuration, cont: K) extends ServiceConfigF[K]

  val algebra: Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams(v)                 => ServiceParams(v)
      case WithServiceName(v, c)         => ServiceParams.serviceName.set(ServiceName(v))(c)
      case WithRestartInterval(v, c)     => ServiceParams.restartInterval.set(RestartInterval(v))(c)
      case WithHealthCheckInterval(v, c) => ServiceParams.healthCheckInterval.set(HealthCheckInterval(v))(c)
    }
}

final case class ServiceConfig(value: Fix[ServiceConfigF]) {
  import ServiceConfigF._

  def withServiceName(sn: String): ServiceConfig                = ServiceConfig(Fix(WithServiceName(sn, value)))
  def withRestartInterval(d: FiniteDuration): ServiceConfig     = ServiceConfig(Fix(WithRestartInterval(d, value)))
  def withHealthCheckInterval(d: FiniteDuration): ServiceConfig = ServiceConfig(Fix(WithHealthCheckInterval(d, value)))
  def evalConfig: ServiceParams                                 = scheme.cata(algebra).apply(value)
}

object ServiceConfig {

  def apply(appName: String): ServiceConfig =
    ServiceConfig(Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]](appName)))
}
