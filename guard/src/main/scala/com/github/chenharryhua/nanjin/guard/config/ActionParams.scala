package com.github.chenharryhua.nanjin.guard.config

import cats.{Applicative, Show}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.guard.MaxRetry
import eu.timepit.refined.cats.*
import io.circe.generic.JsonCodec
import io.circe.refined.*
import monocle.macros.Lenses
import org.typelevel.cats.time.instances.duration
import retry.{RetryPolicies, RetryPolicy}

import java.time.Duration
import scala.jdk.DurationConverters.JavaDurationOps

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

@JsonCodec
final case class ActionParams private (
  spanName: String,
  importance: Importance,
  isCounting: Boolean,
  isTiming: Boolean,
  isExpensive: Boolean,
  retry: ActionRetryParams,
  serviceParams: ServiceParams) {

  val isCritical: Boolean   = importance > Importance.High // Critical
  val isNotice: Boolean     = importance > Importance.Medium // Hight + Critical
  val isNonTrivial: Boolean = importance > Importance.Low // Medium + High + Critical

  val digested: Digested = Digested(serviceParams, spanName)

}

object ActionParams {
  implicit val showActionParams: Show[ActionParams] = cats.derived.semiauto.show[ActionParams]

  def apply(agentParams: AgentParams, spanName: String): ActionParams =
    ActionParams(
      spanName = spanName,
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
