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
  private def jitterBackoff[F[_]: Applicative](maxDelay: FiniteDuration): RetryPolicy[F] =
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
    case JitterBackoff(value) => jitterBackoff[F](value)
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
final case class JitterBackoff(value: FiniteDuration) extends NJRetryPolicy

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
  retryPolicy: NJRetryPolicy,
  shouldTerminate: Boolean
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
    retryPolicy = ConstantDelay(10.seconds),
    shouldTerminate = true
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

  final case class WithTerminate[K](value: Boolean, cont: K) extends ActionConfigF[K]

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
      case WithTerminate(v, c)           => ActionParams.shouldTerminate.set(v)(c)
    }
}

final case class ActionConfig private (value: Fix[ActionConfigF]) {
  import ActionConfigF._

  def slack_succ(v: Boolean): ActionConfig       = ActionConfig(Fix(WithAlertMaskSucc(value = v, value)))
  def slack_succ_on: ActionConfig                = slack_succ(true)
  def slack_succ_off: ActionConfig               = slack_succ(false)
  def slack_fail(v: Boolean): ActionConfig       = ActionConfig(Fix(WithAlertMaskFail(value = v, value)))
  def slack_fail_on: ActionConfig                = slack_fail(true)
  def slack_fail_off: ActionConfig               = slack_fail(false)
  def slack_retry(v: Boolean): ActionConfig      = ActionConfig(Fix(WithAlertMaskRetry(value = v, value)))
  def slack_retry_on: ActionConfig               = slack_retry(true)
  def slack_retry_off: ActionConfig              = slack_retry(false)
  def slack_first_fail(v: Boolean): ActionConfig = ActionConfig(Fix(WithAlertMaskFirstRetry(value = v, value)))
  def slack_first_fail_on: ActionConfig          = slack_first_fail(true)
  def slack_first_fail_off: ActionConfig         = slack_first_fail(false)
  def slack_fyi(v: Boolean): ActionConfig        = ActionConfig(Fix(WithAlertMaskFYI(value = v, value)))
  def slack_fyi_on: ActionConfig                 = slack_fyi(true)
  def slack_fyi_off: ActionConfig                = slack_fyi(false)
  def slack_start(v: Boolean): ActionConfig      = ActionConfig(Fix(WithAlertMaskStart(value = v, value)))
  def slack_start_on: ActionConfig               = slack_start(true)
  def slack_start_off: ActionConfig              = slack_start(false)
  def slack_all: ActionConfig =
    slack_succ_on.slack_fail_on.slack_retry_on.slack_first_fail_on.slack_fyi_on.slack_start_on
  def slack_none: ActionConfig =
    slack_succ_off.slack_fail_off.slack_retry_off.slack_first_fail_off.slack_fyi_off.slack_start_off

  def max_retries(num: Int): ActionConfig = ActionConfig(Fix(WithMaxRetries(num, value)))

  def constant_delay(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(ConstantDelay(delay), value)))

  def exponential_backoff(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(ExponentialBackoff(delay), value)))

  def fibonacci_backoff(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(FibonacciBackoff(delay), value)))

  def full_jitter_backoff(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(FullJitter(delay), value)))

  def non_terminating: ActionConfig =
    ActionConfig(Fix(WithTerminate(value = false, value)))

  def evalConfig: ActionParams = scheme.cata(algebra).apply(value)
}

private[guard] object ActionConfig {

  def apply(serviceParams: ServiceParams): ActionConfig = new ActionConfig(
    Fix(ActionConfigF.InitParams[Fix[ActionConfigF]](serviceParams)))
}
