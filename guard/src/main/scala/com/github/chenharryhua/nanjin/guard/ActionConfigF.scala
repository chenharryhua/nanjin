package com.github.chenharryhua.nanjin.guard

import cats.derived.auto.functor._
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses

import java.time.ZoneId
import scala.concurrent.duration._

@Lenses final case class AlertMask(alertSucc: Boolean, alertFail: Boolean)

@Lenses final case class ActionParams private (
  applicationName: String,
  serviceName: String,
  alertMask: AlertMask,
  maxRetries: Int,
  retryPolicy: NJRetryPolicy,
  zoneId: ZoneId
)

private object ActionParams {

  def apply(): ActionParams = ActionParams(
    applicationName = "unknown",
    serviceName = "unknown",
    alertMask = AlertMask(alertSucc = false, alertFail = false),
    maxRetries = 3,
    retryPolicy = ConstantDelay(10.seconds),
    zoneId = ZoneId.systemDefault()
  )
}

sealed trait ActionConfigF[F]

private object ActionConfigF {
  final case class InitParam[K]() extends ActionConfigF[K]
  final case class WithApplicationName[K](value: String, cont: K) extends ActionConfigF[K]
  final case class WithServiceName[K](value: String, cont: K) extends ActionConfigF[K]

  final case class WithZoneId[K](value: ZoneId, cont: K) extends ActionConfigF[K]

  final case class WithMaxRetries[K](value: Int, cont: K) extends ActionConfigF[K]
  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends ActionConfigF[K]

  final case class WithAlertMaskSucc[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithAlertMaskFail[K](value: Boolean, cont: K) extends ActionConfigF[K]

  val algebra: Algebra[ActionConfigF, ActionParams] =
    Algebra[ActionConfigF, ActionParams] {
      case InitParam()               => ActionParams()
      case WithApplicationName(v, c) => ActionParams.applicationName.set(v)(c)
      case WithServiceName(v, c)     => ActionParams.serviceName.set(v)(c)
      case WithMaxRetries(v, c)      => ActionParams.maxRetries.set(v)(c)
      case WithAlertMaskSucc(v, c)   => ActionParams.alertMask.composeLens(AlertMask.alertSucc).set(v)(c)
      case WithAlertMaskFail(v, c)   => ActionParams.alertMask.composeLens(AlertMask.alertFail).set(v)(c)
      case WithRetryPolicy(v, c)     => ActionParams.retryPolicy.set(v)(c)
      case WithZoneId(v, c)          => ActionParams.zoneId.set(v)(c)
    }
}

final case class ActionConfig private (value: Fix[ActionConfigF]) {
  import ActionConfigF._

  def withApplicationName(an: String): ActionConfig = ActionConfig(Fix(WithApplicationName(an, value)))
  def withServiceName(sn: String): ActionConfig     = ActionConfig(Fix(WithServiceName(sn, value)))

  def withZoneId(tz: ZoneId): ActionConfig = ActionConfig(Fix(WithZoneId(tz, value)))

  def succOn: ActionConfig = ActionConfig(Fix(WithAlertMaskSucc(value = true, value)))
  def failOn: ActionConfig = ActionConfig(Fix(WithAlertMaskFail(value = true, value)))

  def withMaxRetries(n: Int): ActionConfig =
    ActionConfig(Fix(WithMaxRetries(n, value)))

  def constantDelay(v: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(ConstantDelay(v), value)))

  def exponentialBackoff(v: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(ExponentialBackoff(v), value)))

  def fibonacciBackoff(v: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(FibonacciBackoff(v), value)))

  def fullJitter(v: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(FullJitter(v), value)))

  def evalConfig: ActionParams = scheme.cata(algebra).apply(value)
}

private object ActionConfig {
  val default: ActionConfig = new ActionConfig(Fix(ActionConfigF.InitParam[Fix[ActionConfigF]]()))
}
