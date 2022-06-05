package com.github.chenharryhua.nanjin.guard.config

import cats.{Applicative, Show}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.guard.{MaxRetry, Span}
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import eu.timepit.refined.cats.*
import io.circe.generic.JsonCodec
import io.circe.refined.*
import monocle.macros.Lenses
import org.typelevel.cats.time.instances.duration
import retry.{RetryPolicies, RetryPolicy}
import retry.PolicyDecision.DelayAndRetry

import java.time.Duration
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.JavaDurationOps

@JsonCodec
sealed abstract class NJRetryPolicy {
  import NJRetryPolicy.*

  final def policy[F[_]](implicit F: Applicative[F]): RetryPolicy[F] = this match {
    case ConstantDelay(value)      => RetryPolicies.constantDelay[F](value.toScala)
    case ExponentialBackoff(value) => RetryPolicies.exponentialBackoff[F](value.toScala)
    case FibonacciBackoff(value)   => RetryPolicies.fibonacciBackoff[F](value.toScala)
    case FullJitter(value)         => RetryPolicies.fullJitter[F](value.toScala)
    // https://cb372.github.io/cats-retry/docs/policies.html#writing-your-own-policy
    case JitterBackoff(min, max) => jitterBackoff[F](min.toScala, max.toScala)
    case AlwaysGiveUp            => RetryPolicies.alwaysGiveUp[F]
  }
}

object NJRetryPolicy extends duration {
  implicit val showNJRetryPolicy: Show[NJRetryPolicy] = cats.derived.semiauto.show[NJRetryPolicy]

  def jitterBackoff[F[_]: Applicative](min: FiniteDuration, max: FiniteDuration): RetryPolicy[F] =
    RetryPolicy.liftWithShow(
      _ =>
        DelayAndRetry(
          FiniteDuration(
            ThreadLocalRandom.current().nextLong(min.toNanos, max.toNanos),
            TimeUnit.NANOSECONDS)),
      show"jitterBackoff(minDelay=${defaultFormatter.format(min)}, maxDelay=${defaultFormatter.format(max)})"
    )

  // java.time.duration is supported natively by circe
  final case class ConstantDelay(value: Duration) extends NJRetryPolicy
  final case class ExponentialBackoff(value: Duration) extends NJRetryPolicy
  final case class FibonacciBackoff(value: Duration) extends NJRetryPolicy
  final case class FullJitter(value: Duration) extends NJRetryPolicy
  final case class JitterBackoff(min: Duration, max: Duration) extends NJRetryPolicy
  case object AlwaysGiveUp extends NJRetryPolicy
}

@Lenses @JsonCodec final case class ActionRetryParams(
  maxRetries: Option[MaxRetry],
  capDelay: Option[Duration],
  njRetryPolicy: NJRetryPolicy) {
  def policy[F[_]: Applicative]: RetryPolicy[F] =
    maxRetries
      .map(_.value)
      .filter(_ > 0)
      .map(RetryPolicies.limitRetries[F])
      .map { limit =>
        njRetryPolicy match {
          case NJRetryPolicy.ConstantDelay(value) =>
            val p = RetryPolicies.constantDelay[F](value.toScala)
            capDelay.map(d => RetryPolicies.capDelay(d.toScala, p)).getOrElse(p).join(limit)
          case NJRetryPolicy.ExponentialBackoff(value) =>
            val p = RetryPolicies.exponentialBackoff[F](value.toScala)
            capDelay.map(d => RetryPolicies.capDelay(d.toScala, p)).getOrElse(p).join(limit)
          case NJRetryPolicy.FibonacciBackoff(value) =>
            val p = RetryPolicies.fibonacciBackoff[F](value.toScala)
            capDelay.map(d => RetryPolicies.capDelay(d.toScala, p)).getOrElse(p).join(limit)
          case NJRetryPolicy.FullJitter(value) =>
            val p = RetryPolicies.fullJitter[F](value.toScala)
            capDelay.map(d => RetryPolicies.capDelay(d.toScala, p)).getOrElse(p).join(limit)
          case NJRetryPolicy.JitterBackoff(min, max) =>
            val p = NJRetryPolicy.jitterBackoff[F](min.toScala, max.toScala)
            capDelay.map(d => RetryPolicies.capDelay(d.toScala, p)).getOrElse(p).join(limit)
          case NJRetryPolicy.AlwaysGiveUp => RetryPolicies.alwaysGiveUp[F]
        }
      }
      .getOrElse(RetryPolicies.alwaysGiveUp[F])
}

object ActionRetryParams extends duration {
  implicit val showActionRetryParams: Show[ActionRetryParams] = cats.derived.semiauto.show[ActionRetryParams]
}

@JsonCodec
final case class ActionParams private (
  spans: List[Span],
  importance: Importance,
  isCounting: Boolean,
  isTiming: Boolean,
  isExpensive: Boolean,
  retry: ActionRetryParams,
  serviceParams: ServiceParams) {

  val isCritical: Boolean   = importance > Importance.High // Critical
  val isNotice: Boolean     = importance > Importance.Medium // Hight + Critical
  val isNonTrivial: Boolean = importance > Importance.Low // Medium + High + Critical

  val name: Digested = Digested(serviceParams, spans)

}

object ActionParams {
  implicit val showActionParams: Show[ActionParams] = cats.derived.semiauto.show[ActionParams]

  def apply(agentParams: AgentParams): ActionParams =
    ActionParams(
      spans = agentParams.spans,
      importance = agentParams.importance,
      isCounting = agentParams.isCounting,
      isTiming = agentParams.isTiming,
      isExpensive = agentParams.isExpensive,
      retry = ActionRetryParams(
        maxRetries = agentParams.maxRetries,
        capDelay = agentParams.capDelay,
        njRetryPolicy = agentParams.njRetryPolicy),
      serviceParams = agentParams.serviceParams
    )
}
