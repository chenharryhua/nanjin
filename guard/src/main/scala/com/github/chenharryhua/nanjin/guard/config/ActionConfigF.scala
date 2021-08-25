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
  import NJRetryPolicy.*
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
}

private[guard] object NJRetryPolicy {
  implicit val showNJRetryPolicy: Show[NJRetryPolicy] = cats.derived.semiauto.show[NJRetryPolicy]

  final case class ConstantDelay(value: FiniteDuration) extends NJRetryPolicy
  final case class ExponentialBackoff(value: FiniteDuration) extends NJRetryPolicy
  final case class FibonacciBackoff(value: FiniteDuration) extends NJRetryPolicy
  final case class FullJitter(value: FiniteDuration) extends NJRetryPolicy
  final case class JitterBackoff(min: FiniteDuration, max: FiniteDuration) extends NJRetryPolicy
}

@Lenses final case class ActionRetryParams private (
  maxRetries: Int,
  capDelay: Option[FiniteDuration],
  njRetryPolicy: NJRetryPolicy) {
  def policy[F[_]: Applicative]: RetryPolicy[F] =
    capDelay.fold(njRetryPolicy.policy[F].join(RetryPolicies.limitRetries[F](maxRetries)))(cd =>
      RetryPolicies.capDelay[F](cd, njRetryPolicy.policy[F]).join(RetryPolicies.limitRetries[F](maxRetries)))
}

@Lenses final case class ActionParams private (
  actionName: String,
  throughputLevel: ThroughputLevel,
  serviceParams: ServiceParams,
  shouldTerminate: Boolean,
  retry: ActionRetryParams)

object ActionParams {

  def apply(serviceParams: ServiceParams): ActionParams = ActionParams(
    actionName = "anonymous",
    throughputLevel = ThroughputLevel.Medium,
    serviceParams = serviceParams,
    shouldTerminate = true,
    retry = ActionRetryParams(maxRetries = 0, capDelay = None, njRetryPolicy = NJRetryPolicy.ConstantDelay(10.seconds))
  )
}

sealed private[guard] trait ActionConfigF[F]

private object ActionConfigF {
  implicit val functorActionConfigF: Functor[ActionConfigF] = cats.derived.semiauto.functor[ActionConfigF]

  final case class InitParams[K](serviceParams: ServiceParams) extends ActionConfigF[K]

  final case class WithMaxRetries[K](value: Int, cont: K) extends ActionConfigF[K]
  final case class WithCapDelay[K](value: FiniteDuration, cont: K) extends ActionConfigF[K]
  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends ActionConfigF[K]
  final case class WithThroughputLevel[K](value: ThroughputLevel, cont: K) extends ActionConfigF[K]

  final case class WithActionName[K](value: String, cont: K) extends ActionConfigF[K]

  final case class WithTermination[K](value: Boolean, cont: K) extends ActionConfigF[K]

  val algebra: Algebra[ActionConfigF, ActionParams] =
    Algebra[ActionConfigF, ActionParams] {
      case InitParams(v)             => ActionParams(v)
      case WithRetryPolicy(v, c)     => ActionParams.retry.composeLens(ActionRetryParams.njRetryPolicy).set(v)(c)
      case WithMaxRetries(v, c)      => ActionParams.retry.composeLens(ActionRetryParams.maxRetries).set(v)(c)
      case WithCapDelay(v, c)        => ActionParams.retry.composeLens(ActionRetryParams.capDelay).set(Some(v))(c)
      case WithTermination(v, c)     => ActionParams.shouldTerminate.set(v)(c)
      case WithThroughputLevel(v, c) => ActionParams.throughputLevel.set(v)(c)
      case WithActionName(v, c)      => ActionParams.actionName.set(v)(c)
    }
}

final case class ActionConfig private (value: Fix[ActionConfigF]) {
  import ActionConfigF.*

  def withMaxRetries(num: Int): ActionConfig          = ActionConfig(Fix(WithMaxRetries(num, value)))
  def withCapDelay(dur: FiniteDuration): ActionConfig = ActionConfig(Fix(WithCapDelay(dur, value)))

  def withConstantDelay(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(NJRetryPolicy.ConstantDelay(delay), value)))

  def withExponentialBackoff(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(NJRetryPolicy.ExponentialBackoff(delay), value)))

  def withFibonacciBackoff(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(NJRetryPolicy.FibonacciBackoff(delay), value)))

  def withFullJitterBackoff(delay: FiniteDuration): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(NJRetryPolicy.FullJitter(delay), value)))

  def withNonTermination: ActionConfig =
    ActionConfig(Fix(WithTermination(value = false, value)))

  def withLowThroughput: ActionConfig  = ActionConfig(Fix(WithThroughputLevel(ThroughputLevel.Low, value)))
  def withHighThroughput: ActionConfig = ActionConfig(Fix(WithThroughputLevel(ThroughputLevel.High, value)))

  def withActionName(name: String): ActionConfig = ActionConfig(Fix(WithActionName(name, value)))

  def evalConfig: ActionParams = scheme.cata(algebra).apply(value)
}

private[guard] object ActionConfig {

  def apply(serviceParams: ServiceParams): ActionConfig =
    ActionConfig(Fix(ActionConfigF.InitParams[Fix[ActionConfigF]](serviceParams)))
}
