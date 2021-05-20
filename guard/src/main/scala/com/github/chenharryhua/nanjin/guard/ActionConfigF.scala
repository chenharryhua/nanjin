package com.github.chenharryhua.nanjin.guard

import cats.derived.auto.functor._
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses

import scala.concurrent.duration._

@Lenses final case class AlertMask(alertSucc: Boolean, alertFail: Boolean)

@Lenses final case class ActionParams private (
  applicationName: String,
  serviceName: String,
  actionName: String,
  maxRetries: Long,
  retryInterval: FiniteDuration,
  alertMask: AlertMask
)

private object ActionParams {

  def apply(): ActionParams = ActionParams(
    applicationName = "unknown",
    serviceName = "unknown",
    actionName = "unknown",
    maxRetries = 3,
    retryInterval = 10.seconds,
    alertMask = AlertMask(alertSucc = true, alertFail = true)
  )
}

sealed trait ActionConfigF[F]

private object ActionConfigF {
  final case class InitParam[K]() extends ActionConfigF[K]
  final case class WithApplicationName[K](value: String, cont: K) extends ActionConfigF[K]
  final case class WithServiceName[K](value: String, cont: K) extends ActionConfigF[K]
  final case class WithActionName[K](value: String, cont: K) extends ActionConfigF[K]
  final case class WithMaxRetries[K](value: Long, cont: K) extends ActionConfigF[K]
  final case class WithRetryInterval[K](value: FiniteDuration, cont: K) extends ActionConfigF[K]
  final case class WithAlertMaskSucc[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithAlertMaskFail[K](value: Boolean, cont: K) extends ActionConfigF[K]

  val algebra: Algebra[ActionConfigF, ActionParams] =
    Algebra[ActionConfigF, ActionParams] {
      case InitParam()               => ActionParams()
      case WithApplicationName(v, c) => ActionParams.applicationName.set(v)(c)
      case WithServiceName(v, c)     => ActionParams.serviceName.set(v)(c)
      case WithActionName(v, c)      => ActionParams.actionName.set(v)(c)
      case WithMaxRetries(v, c)      => ActionParams.maxRetries.set(v)(c)
      case WithRetryInterval(v, c)   => ActionParams.retryInterval.set(v)(c)
      case WithAlertMaskSucc(v, c)   => ActionParams.alertMask.composeLens(AlertMask.alertSucc).set(v)(c)
      case WithAlertMaskFail(v, c)   => ActionParams.alertMask.composeLens(AlertMask.alertFail).set(v)(c)
    }

}

final class ActionConfig private (value: Fix[ActionConfigF]) {
  import ActionConfigF._

  def withApplicationName(an: String): ActionConfig = new ActionConfig(Fix(WithApplicationName(an, value)))
  def withServiceName(sn: String): ActionConfig     = new ActionConfig(Fix(WithServiceName(sn, value)))
  def withActionName(an: String): ActionConfig      = new ActionConfig(Fix(WithActionName(an, value)))

  def withMaxRetries(n: Long): ActionConfig              = new ActionConfig(Fix(WithMaxRetries(n, value)))
  def withRetryInterval(d: FiniteDuration): ActionConfig = new ActionConfig(Fix(WithRetryInterval(d, value)))

  def offSucc: ActionConfig = new ActionConfig(Fix(WithAlertMaskSucc(value = false, value)))
  def offFail: ActionConfig = new ActionConfig(Fix(WithAlertMaskFail(value = false, value)))

  private[guard] def evalConfig: ActionParams = scheme.cata(algebra).apply(value)
}

private object ActionConfig {
  val default: ActionConfig = new ActionConfig(Fix(ActionConfigF.InitParam[Fix[ActionConfigF]]()))
}
