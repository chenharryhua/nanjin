package com.github.chenharryhua.nanjin.guard.config

import cats.{Applicative, Functor, Show}
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

object NJRetryPolicy {
  implicit val showNJRetryPolicy: Show[NJRetryPolicy] = cats.derived.semiauto.show[NJRetryPolicy]
}

final private case class ConstantDelay(value: FiniteDuration) extends NJRetryPolicy
final private case class ExponentialBackoff(value: FiniteDuration) extends NJRetryPolicy
final private case class FibonacciBackoff(value: FiniteDuration) extends NJRetryPolicy
final private case class FullJitter(value: FiniteDuration) extends NJRetryPolicy

@Lenses final case class AlertMask private (alertSucc: Boolean, alertFail: Boolean)

@Lenses final case class ActionParams private (
  alertMask: AlertMask,
  maxRetries: Int,
  retryPolicy: NJRetryPolicy
)

private object ActionParams {
  implicit val showActionParams: Show[ActionParams] = cats.derived.semiauto.show[ActionParams]

  def apply(): ActionParams = ActionParams(
    alertMask = AlertMask(alertSucc = false, alertFail = true),
    maxRetries = 3,
    retryPolicy = ConstantDelay(10.seconds)
  )
}

sealed trait ActionConfigF[F]

private object ActionConfigF {
  implicit val functorActionConfigF: Functor[ActionConfigF] = cats.derived.semiauto.functor[ActionConfigF]

  final case class InitParam[K]() extends ActionConfigF[K]

  final case class WithMaxRetries[K](value: Int, cont: K) extends ActionConfigF[K]
  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends ActionConfigF[K]

  final case class WithAlertMaskSucc[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithAlertMaskFail[K](value: Boolean, cont: K) extends ActionConfigF[K]

  val algebra: Algebra[ActionConfigF, ActionParams] =
    Algebra[ActionConfigF, ActionParams] {
      case InitParam()             => ActionParams()
      case WithRetryPolicy(v, c)   => ActionParams.retryPolicy.set(v)(c)
      case WithMaxRetries(v, c)    => ActionParams.maxRetries.set(v)(c)
      case WithAlertMaskSucc(v, c) => ActionParams.alertMask.composeLens(AlertMask.alertSucc).set(v)(c)
      case WithAlertMaskFail(v, c) => ActionParams.alertMask.composeLens(AlertMask.alertFail).set(v)(c)
    }
}

final case class ActionConfig private (value: Fix[ActionConfigF]) {
  import ActionConfigF._

  def withSuccAlertOn: ActionConfig  = ActionConfig(Fix(WithAlertMaskSucc(value = true, value)))
  def withSuccAlertOff: ActionConfig = ActionConfig(Fix(WithAlertMaskSucc(value = false, value)))
  def withFailAlertOn: ActionConfig  = ActionConfig(Fix(WithAlertMaskFail(value = true, value)))
  def withFailAlertOff: ActionConfig = ActionConfig(Fix(WithAlertMaskFail(value = false, value)))

  def withMaxRetries(num: Int): ActionConfig = ActionConfig(Fix(WithMaxRetries(num, value)))

  def withConstantDelay(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(ConstantDelay(delay), value)))

  def withExponentialBackoff(delay: FiniteDuration): ActionConfig = ActionConfig(
    Fix(WithRetryPolicy(ExponentialBackoff(delay), value)))

  def withFibonacciBackoff(delay: FiniteDuration): ActionConfig = ActionConfig(
    Fix(WithRetryPolicy(FibonacciBackoff(delay), value)))

  def withFullJitter(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(FullJitter(delay), value)))

  def evalConfig: ActionParams = scheme.cata(algebra).apply(value)
}

private[guard] object ActionConfig {
  val default: ActionConfig = new ActionConfig(Fix(ActionConfigF.InitParam[Fix[ActionConfigF]]()))
}
