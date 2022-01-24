package com.github.chenharryhua.nanjin.guard.config

import cats.syntax.all.*
import cats.{Applicative, Show}
import com.github.chenharryhua.nanjin.datetime.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.datetime.instances.*
import eu.timepit.refined.cats.*
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import io.circe.refined.*
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
  maxRetries: MaxRetry,
  capDelay: Option[FiniteDuration],
  njRetryPolicy: NJRetryPolicy) {
  def policy[F[_]: Applicative]: RetryPolicy[F] =
    capDelay.fold(njRetryPolicy.policy[F].join(RetryPolicies.limitRetries[F](maxRetries.value)))(cd =>
      RetryPolicies.capDelay[F](cd, njRetryPolicy.policy[F]).join(RetryPolicies.limitRetries[F](maxRetries.value)))
}

object ActionRetryParams {
  implicit val showActionRetryParams: Show[ActionRetryParams] = cats.derived.semiauto.show[ActionRetryParams]
}

@JsonCodec
final case class ActionParams private (
  metricName: Digested,
  importance: Importance,
  isCounting: CountAction,
  isTiming: TimeAction,
  isExpensive: ExpensiveAction,
  retry: ActionRetryParams,
  catalog: Catalog,
  serviceParams: ServiceParams) {
  def startTitle: String  = s"$catalog ${metricName.metricRepr} started"
  def retryTitle: String  = s"$catalog ${metricName.metricRepr} retrying"
  def failedTitle: String = s"$catalog ${metricName.metricRepr} failed"
  def succedTitle: String = s"$catalog ${metricName.metricRepr} succed"

  val isCritical: Boolean   = importance > Importance.High // Critical
  val isNotice: Boolean     = importance > Importance.Medium // Hight + Critical
  val isNonTrivial: Boolean = importance > Importance.Low // Medium + High + Critical
}

object ActionParams {
  implicit val showActionParams: Show[ActionParams] = cats.derived.semiauto.show[ActionParams]

  def apply(agentParams: AgentParams): ActionParams =
    ActionParams(
      metricName = Digested(agentParams.spans, agentParams.serviceParams),
      importance = agentParams.importance,
      isCounting = agentParams.isCounting,
      isTiming = agentParams.isTiming,
      isExpensive = agentParams.isExpensive,
      retry = agentParams.retry,
      catalog = agentParams.catalog,
      serviceParams = agentParams.serviceParams
    )
}
