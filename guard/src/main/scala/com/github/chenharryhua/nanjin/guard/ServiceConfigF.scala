package com.github.chenharryhua.nanjin.guard

import cats.derived.auto.functor._
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses

import scala.concurrent.duration._

@Lenses final case class ServiceParams private (
  applicationName: String,
  serviceName: String,
  restartInterval: FiniteDuration,
  healthCheckInterval: FiniteDuration
)

private object ServiceParams {
  def apply(): ServiceParams = ServiceParams("unknown", "unknown", 30.seconds, 6.hours)
}

sealed trait ServiceConfigF[F]

private object ServiceConfigF {

  final case class InitParams[K]() extends ServiceConfigF[K]
  final case class WithApplicationName[K](value: String, cont: K) extends ServiceConfigF[K]
  final case class WithServiceName[K](value: String, cont: K) extends ServiceConfigF[K]
  final case class WithRestartInterval[K](value: FiniteDuration, cont: K) extends ServiceConfigF[K]
  final case class WithHealthCheckInterval[K](value: FiniteDuration, cont: K) extends ServiceConfigF[K]

  val algebra: Algebra[ServiceConfigF, ServiceParams] =
    Algebra[ServiceConfigF, ServiceParams] {
      case InitParams()                  => ServiceParams()
      case WithApplicationName(v, c)     => ServiceParams.applicationName.set(v)(c)
      case WithServiceName(v, c)         => ServiceParams.serviceName.set(v)(c)
      case WithRestartInterval(v, c)     => ServiceParams.restartInterval.set(v)(c)
      case WithHealthCheckInterval(v, c) => ServiceParams.healthCheckInterval.set(v)(c)
    }
}

final case class ServiceConfig private (value: Fix[ServiceConfigF]) {
  import ServiceConfigF._

  def withApplicationName(an: String): ServiceConfig            = ServiceConfig(Fix(WithApplicationName(an, value)))
  def withServiceName(sn: String): ServiceConfig                = ServiceConfig(Fix(WithServiceName(sn, value)))
  def withRestartInterval(d: FiniteDuration): ServiceConfig     = ServiceConfig(Fix(WithRestartInterval(d, value)))
  def withHealthCheckInterval(d: FiniteDuration): ServiceConfig = ServiceConfig(Fix(WithHealthCheckInterval(d, value)))
  def evalConfig: ServiceParams                                 = scheme.cata(algebra).apply(value)
}

private object ServiceConfig {

  def apply(): ServiceConfig = ServiceConfig(Fix(ServiceConfigF.InitParams[Fix[ServiceConfigF]]()))
}
