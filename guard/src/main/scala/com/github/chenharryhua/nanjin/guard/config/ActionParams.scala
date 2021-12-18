package com.github.chenharryhua.nanjin.guard.config

import cats.syntax.show.*
import cats.{Applicative, Show}
import com.github.chenharryhua.nanjin.datetime.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.datetime.instances.*
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import monocle.macros.Lenses
import retry.PolicyDecision.DelayAndRetry
import retry.{RetryPolicies, RetryPolicy}

import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import scala.concurrent.duration.*

@JsonCodec
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

object NJRetryPolicy {
  implicit val showNJRetryPolicy: Show[NJRetryPolicy] = cats.derived.semiauto.show[NJRetryPolicy]

  final case class ConstantDelay(value: FiniteDuration) extends NJRetryPolicy
  final case class ExponentialBackoff(value: FiniteDuration) extends NJRetryPolicy
  final case class FibonacciBackoff(value: FiniteDuration) extends NJRetryPolicy
  final case class FullJitter(value: FiniteDuration) extends NJRetryPolicy
  final case class JitterBackoff(min: FiniteDuration, max: FiniteDuration) extends NJRetryPolicy
}

@Lenses @JsonCodec final case class ActionRetryParams(
  maxRetries: Int,
  capDelay: Option[FiniteDuration],
  njRetryPolicy: NJRetryPolicy) {
  def policy[F[_]: Applicative]: RetryPolicy[F] =
    capDelay.fold(njRetryPolicy.policy[F].join(RetryPolicies.limitRetries[F](maxRetries)))(cd =>
      RetryPolicies.capDelay[F](cd, njRetryPolicy.policy[F]).join(RetryPolicies.limitRetries[F](maxRetries)))
}

object ActionRetryParams {
  implicit val showActionRetryParams: Show[ActionRetryParams] = cats.derived.semiauto.show[ActionRetryParams]
}

@Lenses @JsonCodec
final case class ActionParams(
  guardId: GuardId,
  serviceParams: ServiceParams,
  importance: Importance,
  isTerminate: Boolean,
  retry: ActionRetryParams)

object ActionParams {
  implicit val showActionParams: Show[ActionParams] = cats.derived.semiauto.show[ActionParams]
  def apply(params: AgentParams): ActionParams =
    ActionParams(GuardId(params), params.serviceParams, params.importance, params.isTerminate, params.retry)
}
