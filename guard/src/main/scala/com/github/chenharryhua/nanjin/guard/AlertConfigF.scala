package com.github.chenharryhua.nanjin.guard

import higherkindness.droste.Algebra
import higherkindness.droste.data.Fix
import monocle.macros.Lenses

import scala.concurrent.duration._

final private case class ServiceAlertEveryNRetries(value: Int) extends AnyVal
final private case class ServiceRestartInterval(value: FiniteDuration) extends AnyVal
final private case class HealthCheckInterval(value: FiniteDuration) extends AnyVal

final private case class ActionMaximumRetries(value: Long) extends AnyVal
final private case class ActionRetryInterval(value: FiniteDuration) extends AnyVal

final private case class AlertLevel2(logSucc: Boolean, logFail: Boolean)

sealed private trait AlertLevel
private case object AlertFailOnly extends AlertLevel
private case object AlertSuccOnly extends AlertLevel
private case object AlertBoth extends AlertLevel

@Lenses final private case class AlertParams(
  applicationName: ApplicationName,
  serviceName: ServiceName,
  serviceAlertEveryNRetries: ServiceAlertEveryNRetries,
  serviceRestartInterval: ServiceRestartInterval,
  healthCheckInterval: HealthCheckInterval,
  actionMaximumRetries: ActionMaximumRetries,
  actionRetryInterval: ActionRetryInterval
)

private object AlertParams {

  def apply(appName: String): AlertParams = AlertParams(
    ApplicationName(appName),
    ServiceName(""),
    ServiceAlertEveryNRetries(30), // 15 minutes raise an alert
    ServiceRestartInterval(30.seconds),
    HealthCheckInterval(6.hours),
    ActionMaximumRetries(3),
    ActionRetryInterval(10.seconds)
  )
}

sealed trait AlertConfigF[F]

object AlertConfigF {
  sealed trait AlertLevel
  case object AlertFailOnly extends AlertLevel
  case object AlertSuccOnly extends AlertLevel
  case object AlertBoth extends AlertLevel

  final case class InitParams[K](value: String) extends AlertConfigF[K]
  final case class WithServiceName[K](value: String, cont: K) extends AlertConfigF[K]
  final case class WithServiceAlertEveryNRetries[K](value: Int, cont: K) extends AlertConfigF[K]
  final case class WithServiceRestartInterval[K](value: FiniteDuration, cont: K) extends AlertConfigF[K]
  final case class WithHealthCheckInterval[K](value: FiniteDuration, cont: K) extends AlertConfigF[K]
  final case class WithActionMaximumRetries[K](value: Long, cont: K) extends AlertConfigF[K]
  final case class WithActionRetryInterval[K](value: FiniteDuration, cont: K) extends AlertConfigF[K]

  private val algebra: Algebra[AlertConfigF, AlertParams] =
    Algebra[AlertConfigF, AlertParams] {
      case InitParams(v)         => AlertParams(v)
      case WithServiceName(v, c) => AlertParams.serviceName.set(ServiceName(v))(c)
      case WithServiceAlertEveryNRetries(v, c) =>
        AlertParams.serviceAlertEveryNRetries.set(ServiceAlertEveryNRetries(v))(c)
      case WithServiceRestartInterval(v, c) =>
        AlertParams.serviceRestartInterval.set(ServiceRestartInterval(v))(c)
      case WithHealthCheckInterval(v, c)  => AlertParams.healthCheckInterval.set(HealthCheckInterval(v))(c)
      case WithActionMaximumRetries(v, c) => AlertParams.actionMaximumRetries.set(ActionMaximumRetries(v))(c)
      case WithActionRetryInterval(v, c)  => AlertParams.actionRetryInterval.set(ActionRetryInterval(v))(c)
    }

  //def evalConfig(cfg: HoarderConfig): HoarderParams = scheme.cata(algebra).apply(cfg.value)

}
final case class AlertConfig(value: AlertConfigF[Fix[AlertConfigF]])
