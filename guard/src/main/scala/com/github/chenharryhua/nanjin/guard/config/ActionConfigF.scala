package com.github.chenharryhua.nanjin.guard.config

import cats.syntax.show.*
import cats.{Applicative, Functor, Show}
import com.github.chenharryhua.nanjin.datetime.DurationFormatter.defaultFormatter
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses
import retry.PolicyDecision.DelayAndRetry
import retry.{RetryPolicies, RetryPolicy}

import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import scala.concurrent.duration.*

sealed abstract class NJRetryPolicy {
  private def jitterBackoff[F[_]: Applicative](min: FiniteDuration, max: FiniteDuration): RetryPolicy[F] =
    RetryPolicy.liftWithShow(
      _ =>
        DelayAndRetry(
          FiniteDuration(ThreadLocalRandom.current().nextLong(min.toNanos, max.toNanos), TimeUnit.NANOSECONDS)),
      show"jitterBackoff(minDelay=${defaultFormatter.format(min)}, maxDelay=${defaultFormatter.format(max)})"
    )

  final def policy[F[_]](implicit F: Applicative[F]): RetryPolicy[F] = this match {
    case ConstantDelay(value)      => RetryPolicies.constantDelay(value)
    case ExponentialBackoff(value) => RetryPolicies.exponentialBackoff(value)
    case FibonacciBackoff(value)   => RetryPolicies.fibonacciBackoff(value)
    case FullJitter(value)         => RetryPolicies.fullJitter(value)
    // https://cb372.github.io/cats-retry/docs/policies.html#writing-your-own-policy
    case JitterBackoff(min, max) => jitterBackoff[F](min, max)
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
final case class JitterBackoff(min: FiniteDuration, max: FiniteDuration) extends NJRetryPolicy {
  override val value: FiniteDuration = max
}

@Lenses final case class SlackAlertMask private (
  alertSucc: Boolean,
  alertFail: Boolean,
  alertRetry: Boolean, // alert every retry
  alertFirstRetry: Boolean, // alert first time failure of the action
  alertStart: Boolean) // alert action start

@Lenses final case class ActionRetryParams private (
  maxRetries: Int,
  capDelay: Option[FiniteDuration],
  njRetryPolicy: NJRetryPolicy) {
  def policy[F[_]: Applicative]: RetryPolicy[F] =
    capDelay.fold(njRetryPolicy.policy[F].join(RetryPolicies.limitRetries[F](maxRetries)))(cd =>
      RetryPolicies.capDelay[F](cd, njRetryPolicy.policy[F]).join(RetryPolicies.limitRetries[F](maxRetries)))
}

@Lenses final case class ActionParams private (
  serviceParams: ServiceParams,
  alertMask: SlackAlertMask, // whether display succNotes and failNotes
  shouldTerminate: Boolean,
  retry: ActionRetryParams)

object ActionParams {

  def apply(serviceParams: ServiceParams): ActionParams = ActionParams(
    serviceParams = serviceParams,
    alertMask = SlackAlertMask(
      alertSucc = false,
      alertFail = true,
      alertRetry = false,
      alertFirstRetry = false,
      alertStart = false),
    shouldTerminate = true,
    retry = ActionRetryParams(maxRetries = 0, capDelay = None, njRetryPolicy = ConstantDelay(10.seconds))
  )
}

sealed private[guard] trait ActionConfigF[F]

private object ActionConfigF {
  implicit val functorActionConfigF: Functor[ActionConfigF] = cats.derived.semiauto.functor[ActionConfigF]

  final case class InitParams[K](serviceParams: ServiceParams) extends ActionConfigF[K]

  final case class WithMaxRetries[K](value: Int, cont: K) extends ActionConfigF[K]
  final case class WithCapDelay[K](value: FiniteDuration, cont: K) extends ActionConfigF[K]
  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends ActionConfigF[K]

  final case class WithAlertMaskSucc[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithAlertMaskFail[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithAlertMaskRetry[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithAlertMaskFirstRetry[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithAlertMaskStart[K](value: Boolean, cont: K) extends ActionConfigF[K]

  final case class WithTermination[K](value: Boolean, cont: K) extends ActionConfigF[K]

  val algebra: Algebra[ActionConfigF, ActionParams] =
    Algebra[ActionConfigF, ActionParams] {
      case InitParams(v)                 => ActionParams(v)
      case WithRetryPolicy(v, c)         => ActionParams.retry.composeLens(ActionRetryParams.njRetryPolicy).set(v)(c)
      case WithMaxRetries(v, c)          => ActionParams.retry.composeLens(ActionRetryParams.maxRetries).set(v)(c)
      case WithCapDelay(v, c)            => ActionParams.retry.composeLens(ActionRetryParams.capDelay).set(Some(v))(c)
      case WithAlertMaskSucc(v, c)       => ActionParams.alertMask.composeLens(SlackAlertMask.alertSucc).set(v)(c)
      case WithAlertMaskFail(v, c)       => ActionParams.alertMask.composeLens(SlackAlertMask.alertFail).set(v)(c)
      case WithAlertMaskRetry(v, c)      => ActionParams.alertMask.composeLens(SlackAlertMask.alertRetry).set(v)(c)
      case WithAlertMaskFirstRetry(v, c) => ActionParams.alertMask.composeLens(SlackAlertMask.alertFirstRetry).set(v)(c)
      case WithAlertMaskStart(v, c)      => ActionParams.alertMask.composeLens(SlackAlertMask.alertStart).set(v)(c)
      case WithTermination(v, c)         => ActionParams.shouldTerminate.set(v)(c)
    }
}

final case class ActionConfig private (value: Fix[ActionConfigF]) {
  import ActionConfigF.*

  def withSlackSucc(v: Boolean): ActionConfig      = ActionConfig(Fix(WithAlertMaskSucc(value = v, value)))
  def withSlackSuccOn: ActionConfig                = withSlackSucc(true)
  def withSlackSuccOff: ActionConfig               = withSlackSucc(false)
  def withSlackFail(v: Boolean): ActionConfig      = ActionConfig(Fix(WithAlertMaskFail(value = v, value)))
  def withSlackFailOn: ActionConfig                = withSlackFail(true)
  def withSlackFailOff: ActionConfig               = withSlackFail(false)
  def withSlackRetry(v: Boolean): ActionConfig     = ActionConfig(Fix(WithAlertMaskRetry(value = v, value)))
  def withSlackRetryOn: ActionConfig               = withSlackRetry(true)
  def withSlackRetryOff: ActionConfig              = withSlackRetry(false)
  def withSlackFirstFail(v: Boolean): ActionConfig = ActionConfig(Fix(WithAlertMaskFirstRetry(value = v, value)))
  def withSlackFirstFailOn: ActionConfig           = withSlackFirstFail(true)
  def withSlackFirstFailOff: ActionConfig          = withSlackFirstFail(false)
  def withSlackStart(v: Boolean): ActionConfig     = ActionConfig(Fix(WithAlertMaskStart(value = v, value)))
  def withSlackStartOn: ActionConfig               = withSlackStart(true)
  def withSlackStartOff: ActionConfig              = withSlackStart(false)
  def withSlackAll: ActionConfig =
    withSlackSuccOn.withSlackFailOn.withSlackRetryOn.withSlackFirstFailOn.withSlackStartOn
  def withSlackNone: ActionConfig =
    withSlackSuccOff.withSlackFailOff.withSlackRetryOff.withSlackFirstFailOff.withSlackStartOff

  def withMaxRetries(num: Int): ActionConfig          = ActionConfig(Fix(WithMaxRetries(num, value)))
  def withCapDelay(dur: FiniteDuration): ActionConfig = ActionConfig(Fix(WithCapDelay(dur, value)))

  def withConstantDelay(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(ConstantDelay(delay), value)))

  def withExponentialBackoff(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(ExponentialBackoff(delay), value)))

  def withFibonacciBackoff(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(FibonacciBackoff(delay), value)))

  def withFullJitterBackoff(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(FullJitter(delay), value)))

  def withNonTermination: ActionConfig =
    ActionConfig(Fix(WithTermination(value = false, value)))

  def evalConfig: ActionParams = scheme.cata(algebra).apply(value)
}

private[guard] object ActionConfig {

  def apply(serviceParams: ServiceParams): ActionConfig =
    ActionConfig(Fix(ActionConfigF.InitParams[Fix[ActionConfigF]](serviceParams)))
}
