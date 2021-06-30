package com.github.chenharryhua.nanjin.guard.config

import cats.syntax.show._
import cats.{Applicative, Functor, Show}
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import retry.PolicyDecision.DelayAndRetry
import retry.{RetryPolicies, RetryPolicy}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.util.Random

sealed abstract class NJRetryPolicy {
  private def jitter[F[_]: Applicative](maxDelay: FiniteDuration): RetryPolicy[F] =
    RetryPolicy.liftWithShow(
      { _ =>
        val delay = Random.nextInt(maxDelay.toMillis.toInt).toLong
        DelayAndRetry(new FiniteDuration(delay, TimeUnit.MILLISECONDS))
      },
      show"Jitter(maxDelay=$maxDelay)"
    )

  final def policy[F[_]](implicit F: Applicative[F]): RetryPolicy[F] = this match {
    case ConstantDelay(value)      => RetryPolicies.constantDelay(value)
    case ExponentialBackoff(value) => RetryPolicies.exponentialBackoff(value)
    case FibonacciBackoff(value)   => RetryPolicies.fibonacciBackoff(value)
    case FullJitter(value)         => RetryPolicies.fullJitter(value)
    // https://cb372.github.io/cats-retry/docs/policies.html#writing-your-own-policy
    case Jitter(value) => jitter[F](value)
  }
  def value: FiniteDuration
}

object NJRetryPolicy {
  implicit val showNJRetryPolicy: Show[NJRetryPolicy] = cats.derived.semiauto.show[NJRetryPolicy]
}

final case class ConstantDelay(value: FiniteDuration) extends NJRetryPolicy
final case class ExponentialBackoff(value: FiniteDuration) extends NJRetryPolicy
final case class FibonacciBackoff(value: FiniteDuration) extends NJRetryPolicy
final case class FullJitter(value: FiniteDuration) extends NJRetryPolicy
final case class Jitter(value: FiniteDuration) extends NJRetryPolicy

@Lenses final case class SlackAlertMask private (
  alertSucc: Boolean,
  alertFail: Boolean,
  alertRetry: Boolean, // alert every retry
  alertFirstRetry: Boolean, // alert when first time failure of the action
  alertFYI: Boolean,
  alertStart: Boolean)

@Lenses final case class ActionParams private (
  serviceParams: ServiceParams,
  alertMask: SlackAlertMask, // whether display succNotes and failNotes
  maxRetries: Int,
  retryPolicy: NJRetryPolicy
)

object ActionParams {

  def apply(serviceParams: ServiceParams): ActionParams = ActionParams(
    serviceParams = serviceParams,
    alertMask = SlackAlertMask(
      alertSucc = false,
      alertFail = true,
      alertRetry = false,
      alertFirstRetry = false,
      alertFYI = true,
      alertStart = false),
    maxRetries = 3,
    retryPolicy = ConstantDelay(10.seconds)
  )
}

sealed private[guard] trait ActionConfigF[F]

private object ActionConfigF {
  implicit val functorActionConfigF: Functor[ActionConfigF] = cats.derived.semiauto.functor[ActionConfigF]

  final case class InitParams[K](serviceParams: ServiceParams) extends ActionConfigF[K]

  final case class WithMaxRetries[K](value: Int, cont: K) extends ActionConfigF[K]
  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends ActionConfigF[K]

  final case class WithAlertMaskSucc[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithAlertMaskFail[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithAlertMaskRetry[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithAlertMaskFirstRetry[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithAlertMaskFYI[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithAlertMaskStart[K](value: Boolean, cont: K) extends ActionConfigF[K]

  val algebra: Algebra[ActionConfigF, ActionParams] =
    Algebra[ActionConfigF, ActionParams] {
      case InitParams(v)                 => ActionParams(v)
      case WithRetryPolicy(v, c)         => ActionParams.retryPolicy.set(v)(c)
      case WithMaxRetries(v, c)          => ActionParams.maxRetries.set(v)(c)
      case WithAlertMaskSucc(v, c)       => ActionParams.alertMask.composeLens(SlackAlertMask.alertSucc).set(v)(c)
      case WithAlertMaskFail(v, c)       => ActionParams.alertMask.composeLens(SlackAlertMask.alertFail).set(v)(c)
      case WithAlertMaskRetry(v, c)      => ActionParams.alertMask.composeLens(SlackAlertMask.alertRetry).set(v)(c)
      case WithAlertMaskFirstRetry(v, c) => ActionParams.alertMask.composeLens(SlackAlertMask.alertFirstRetry).set(v)(c)
      case WithAlertMaskFYI(v, c)        => ActionParams.alertMask.composeLens(SlackAlertMask.alertFYI).set(v)(c)
      case WithAlertMaskStart(v, c)      => ActionParams.alertMask.composeLens(SlackAlertMask.alertStart).set(v)(c)
    }
}

final case class ActionConfig private (value: Fix[ActionConfigF]) {
  import ActionConfigF._

  def withSuccAlert(v: Boolean): ActionConfig = ActionConfig(Fix(WithAlertMaskSucc(value = v, value)))
  def withSuccAlertOn: ActionConfig           = withSuccAlert(true)
  def withSuccAlertOff: ActionConfig          = withSuccAlert(false)
  def withFailAlert(v: Boolean): ActionConfig = ActionConfig(Fix(WithAlertMaskFail(value = v, value)))
  def withFailAlertOn: ActionConfig           = withFailAlert(true)
  def withFailAlertOff: ActionConfig          = withFailAlert(false)
  def withRetryAlertOn: ActionConfig          = ActionConfig(Fix(WithAlertMaskRetry(value = true, value)))
  def withFirstFailAlertOn: ActionConfig      = ActionConfig(Fix(WithAlertMaskFirstRetry(value = true, value)))
  def withFYIAlertOff: ActionConfig           = ActionConfig(Fix(WithAlertMaskFYI(value = false, value)))
  def withStartAlertOn: ActionConfig          = ActionConfig(Fix(WithAlertMaskStart(value = true, value)))

  def withMaxRetries(num: Int): ActionConfig = ActionConfig(Fix(WithMaxRetries(num, value)))

  def withConstantDelay(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(ConstantDelay(delay), value)))

  def withExponentialBackoff(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(ExponentialBackoff(delay), value)))

  def withFibonacciBackoff(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(FibonacciBackoff(delay), value)))

  def withFullJitter(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(FullJitter(delay), value)))

  def evalConfig: ActionParams = scheme.cata(algebra).apply(value)
}

private[guard] object ActionConfig {

  def apply(serviceParams: ServiceParams): ActionConfig = new ActionConfig(
    Fix(ActionConfigF.InitParams[Fix[ActionConfigF]](serviceParams)))
}
