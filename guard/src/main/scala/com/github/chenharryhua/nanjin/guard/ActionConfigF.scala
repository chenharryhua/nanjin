package com.github.chenharryhua.nanjin.guard

import cats.derived.auto.functor._
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses

import scala.concurrent.duration._

final case class ActionMaxRetries(value: Long) extends AnyVal
final case class ActionRetryInterval(value: FiniteDuration) extends AnyVal

@Lenses final case class AlertMask(alertSucc: Boolean, alertFail: Boolean)

@Lenses final case class ActionParams(
  applicationName: ApplicationName,
  serviceName: ServiceName,
  actionMaxRetries: ActionMaxRetries,
  actionRetryInterval: ActionRetryInterval,
  alertMask: AlertMask
)

object ActionParams {

  def apply(applicationName: String): ActionParams = ActionParams(
    ApplicationName(applicationName),
    ServiceName("unknown"),
    ActionMaxRetries(3),
    ActionRetryInterval(10.seconds),
    AlertMask(alertSucc = true, alertFail = true)
  )
}

sealed trait ActionConfigF[F]

object ActionConfigF {
  final case class InitParams[K](value: String) extends ActionConfigF[K]
  final case class WithServiceName[K](value: String, cont: K) extends ActionConfigF[K]
  final case class WithActionMaxRetries[K](value: Long, cont: K) extends ActionConfigF[K]
  final case class WithActionRetryInterval[K](value: FiniteDuration, cont: K) extends ActionConfigF[K]
  final case class WithAlertMaskSucc[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithAlertMaskFail[K](value: Boolean, cont: K) extends ActionConfigF[K]

  val algebra: Algebra[ActionConfigF, ActionParams] =
    Algebra[ActionConfigF, ActionParams] {
      case InitParams(v)                 => ActionParams(v)
      case WithServiceName(v, c)         => ActionParams.serviceName.set(ServiceName(v))(c)
      case WithActionMaxRetries(v, c)    => ActionParams.actionMaxRetries.set(ActionMaxRetries(v))(c)
      case WithActionRetryInterval(v, c) => ActionParams.actionRetryInterval.set(ActionRetryInterval(v))(c)
      case WithAlertMaskSucc(v, c)       => ActionParams.alertMask.composeLens(AlertMask.alertSucc).set(v)(c)
      case WithAlertMaskFail(v, c)       => ActionParams.alertMask.composeLens(AlertMask.alertFail).set(v)(c)
    }

}

final case class ActionConfig(value: Fix[ActionConfigF]) {
  import ActionConfigF._

  def withServiceName(sn: String): ActionConfig                = ActionConfig(Fix(WithServiceName(sn, value)))
  def withActionMaxRetries(n: Long): ActionConfig              = ActionConfig(Fix(WithActionMaxRetries(n, value)))
  def withActionRetryInterval(d: FiniteDuration): ActionConfig = ActionConfig(Fix(WithActionRetryInterval(d, value)))

  def offSucc: ActionConfig = ActionConfig(Fix(WithAlertMaskSucc(value = false, value)))
  def offFail: ActionConfig = ActionConfig(Fix(WithAlertMaskFail(value = false, value)))

  def evalConfig: ActionParams = scheme.cata(algebra).apply(value)
}

object ActionConfig {

  def apply(applicationName: String): ActionConfig =
    ActionConfig(Fix(ActionConfigF.InitParams[Fix[ActionConfigF]](applicationName)))
}
