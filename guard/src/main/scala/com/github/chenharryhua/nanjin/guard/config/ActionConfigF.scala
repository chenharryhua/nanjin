package com.github.chenharryhua.nanjin.guard.config

import cats.{Applicative, Functor, Show}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.guard.MaxRetry
import eu.timepit.refined.cats.*
import higherkindness.droste.{scheme, Algebra}
import higherkindness.droste.data.Fix
import io.circe.generic.JsonCodec
import io.circe.refined.*
import monocle.macros.Lenses
import org.typelevel.cats.time.instances.duration
import retry.{RetryPolicies, RetryPolicy}

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

@Lenses @JsonCodec final case class ActionRetryParams(
  maxRetries: Option[MaxRetry],
  capDelay: Option[Duration],
  njRetryPolicy: NJRetryPolicy) {

  private def caping[F[_]: Applicative](policy: RetryPolicy[F], limit: RetryPolicy[F]): RetryPolicy[F] =
    capDelay.fold(policy)(d => RetryPolicies.capDelay(d.toScala, policy)).join(limit)

  def policy[F[_]: Applicative]: RetryPolicy[F] =
    maxRetries
      .map(_.value)
      .filter(_ > 0)
      .map(RetryPolicies.limitRetries[F])
      .map { limit =>
        njRetryPolicy match {
          case NJRetryPolicy.ConstantDelay(value) =>
            caping(RetryPolicies.constantDelay[F](value.toScala), limit)
          case NJRetryPolicy.ExponentialBackoff(value) =>
            caping(RetryPolicies.exponentialBackoff[F](value.toScala), limit)
          case NJRetryPolicy.FibonacciBackoff(value) =>
            caping(RetryPolicies.fibonacciBackoff[F](value.toScala), limit)
          case NJRetryPolicy.FullJitter(value) =>
            caping(RetryPolicies.fullJitter[F](value.toScala), limit)
          case NJRetryPolicy.JitterBackoff(min, max) =>
            caping(NJRetryPolicy.jitterBackoff[F](min.toScala, max.toScala), limit)
          case NJRetryPolicy.AlwaysGiveUp => RetryPolicies.alwaysGiveUp[F]
        }
      }
      .getOrElse(RetryPolicies.alwaysGiveUp[F])
}

object ActionRetryParams extends duration {
  implicit val showActionRetryParams: Show[ActionRetryParams] = cats.derived.semiauto.show[ActionRetryParams]
}

@JsonCodec @Lenses
final case class ActionParams private (
  name: String,
  importance: Importance,
  isCounting: Boolean,
  isTiming: Boolean,
  retry: ActionRetryParams,
  serviceParams: ServiceParams) {

  val isCritical: Boolean   = importance > Importance.High // Critical
  val isNotice: Boolean     = importance > Importance.Medium // Hight + Critical
  val isNonTrivial: Boolean = importance > Importance.Low // Medium + High + Critical

  val digested: Digested = Digested(serviceParams, name)

}

object ActionParams {
  implicit val showActionParams: Show[ActionParams] = cats.derived.semiauto.show

  def apply(name: String, serviceParams: ServiceParams): ActionParams =
    ActionParams(
      name = name,
      importance = Importance.Medium,
      isCounting = false,
      isTiming = false,
      retry =
        ActionRetryParams(maxRetries = None, capDelay = None, njRetryPolicy = NJRetryPolicy.AlwaysGiveUp),
      serviceParams = serviceParams
    )
}

sealed private[guard] trait ActionConfigF[X]

private object ActionConfigF {
  implicit val functorActionConfigF: Functor[ActionConfigF] = cats.derived.semiauto.functor[ActionConfigF]

  final case class InitParams[K](name: String, serviceParams: ServiceParams) extends ActionConfigF[K]

  final case class WithCapDelay[K](value: Duration, cont: K) extends ActionConfigF[K]
  final case class WithRetryPolicy[K](policy: NJRetryPolicy, max: Option[MaxRetry], cont: K)
      extends ActionConfigF[K]

  final case class WithImportance[K](value: Importance, cont: K) extends ActionConfigF[K]
  final case class WithTiming[K](value: Boolean, cont: K) extends ActionConfigF[K]
  final case class WithCounting[K](value: Boolean, cont: K) extends ActionConfigF[K]

  val algebra: Algebra[ActionConfigF, ActionParams] =
    Algebra[ActionConfigF, ActionParams] {
      case InitParams(name, sp) => ActionParams(name, sp)
      case WithRetryPolicy(p, m, c) =>
        ActionParams.retry
          .composeLens(ActionRetryParams.njRetryPolicy)
          .set(p)
          .andThen(ActionParams.retry.composeLens(ActionRetryParams.maxRetries).set(m))(c)
      case WithCapDelay(v, c)   => ActionParams.retry.composeLens(ActionRetryParams.capDelay).set(Some(v))(c)
      case WithImportance(v, c) => ActionParams.importance.set(v)(c)
      case WithTiming(v, c)     => ActionParams.isTiming.set(v)(c)
      case WithCounting(v, c)   => ActionParams.isCounting.set(v)(c)
    }
}

final case class ActionConfig private (value: Fix[ActionConfigF]) {
  import ActionConfigF.*

  def withCapDelay(fd: FiniteDuration): ActionConfig = ActionConfig(Fix(WithCapDelay(fd.toJava, value)))

  def withConstantDelay(baseDelay: FiniteDuration, max: MaxRetry): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(NJRetryPolicy.ConstantDelay(baseDelay.toJava), Some(max), value)))

  def withExponentialBackoff(baseDelay: FiniteDuration, max: MaxRetry): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(NJRetryPolicy.ExponentialBackoff(baseDelay.toJava), Some(max), value)))

  def withFibonacciBackoff(baseDelay: FiniteDuration, max: MaxRetry): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(NJRetryPolicy.FibonacciBackoff(baseDelay.toJava), Some(max), value)))

  def withFullJitterBackoff(baseDelay: FiniteDuration, max: MaxRetry): ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(NJRetryPolicy.FullJitter(baseDelay.toJava), Some(max), value)))

  def withAlwaysGiveUp: ActionConfig =
    ActionConfig(Fix(WithRetryPolicy(NJRetryPolicy.AlwaysGiveUp, None, value)))

  def trivial: ActionConfig  = ActionConfig(Fix(WithImportance(Importance.Low, value)))
  def silent: ActionConfig   = ActionConfig(Fix(WithImportance(Importance.Medium, value)))
  def notice: ActionConfig   = ActionConfig(Fix(WithImportance(Importance.High, value)))
  def critical: ActionConfig = ActionConfig(Fix(WithImportance(Importance.Critical, value)))

  def withCounting: ActionConfig    = ActionConfig(Fix(WithCounting(value = true, value)))
  def withTiming: ActionConfig      = ActionConfig(Fix(WithTiming(value = true, value)))
  def withoutCounting: ActionConfig = ActionConfig(Fix(WithCounting(value = false, value)))
  def withoutTiming: ActionConfig   = ActionConfig(Fix(WithTiming(value = false, value)))

  def evalConfig: ActionParams = scheme.cata(algebra).apply(value)
}

private[guard] object ActionConfig {

  def apply(sp: ServiceParams, name: String): ActionConfig = ActionConfig(
    Fix(ActionConfigF.InitParams[Fix[ActionConfigF]](name, sp)))
}
