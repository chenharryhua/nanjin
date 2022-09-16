package com.github.chenharryhua.nanjin.guard.config

import cats.{Applicative, Show}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import io.circe.generic.JsonCodec
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
