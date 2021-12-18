package com.github.chenharryhua.nanjin.guard.config

import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.datetime.instances.*
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import monocle.macros.Lenses

import scala.concurrent.duration.*

@Lenses @JsonCodec final case class AgentParams(
  spans: List[String],
  importance: Importance,
  serviceParams: ServiceParams,
  isTerminate: Boolean,
  retry: ActionRetryParams)

object AgentParams {
  implicit val showAgentParams: Show[AgentParams] = cats.derived.semiauto.show[AgentParams]

  def apply(serviceParams: ServiceParams): AgentParams = AgentParams(
    spans = Nil,
    importance = Importance.Medium,
    serviceParams = serviceParams,
    isTerminate = true,
    retry = ActionRetryParams(maxRetries = 0, capDelay = None, njRetryPolicy = NJRetryPolicy.ConstantDelay(10.seconds))
  )
}

sealed private[guard] trait AgentConfigF[F]

private object AgentConfigF {
  implicit val functorActionConfigF: Functor[AgentConfigF] = cats.derived.semiauto.functor[AgentConfigF]

  final case class InitParams[K](serviceParams: ServiceParams) extends AgentConfigF[K]

  final case class WithMaxRetries[K](value: Int, cont: K) extends AgentConfigF[K]
  final case class WithCapDelay[K](value: FiniteDuration, cont: K) extends AgentConfigF[K]
  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends AgentConfigF[K]
  final case class WithImportance[K](value: Importance, cont: K) extends AgentConfigF[K]

  final case class WithSpans[K](value: List[String], cont: K) extends AgentConfigF[K]

  final case class WithTermination[K](value: Boolean, cont: K) extends AgentConfigF[K]

  val algebra: Algebra[AgentConfigF, AgentParams] =
    Algebra[AgentConfigF, AgentParams] {
      case InitParams(v)         => AgentParams(v)
      case WithRetryPolicy(v, c) => AgentParams.retry.composeLens(ActionRetryParams.njRetryPolicy).set(v)(c)
      case WithMaxRetries(v, c)  => AgentParams.retry.composeLens(ActionRetryParams.maxRetries).set(v)(c)
      case WithCapDelay(v, c)    => AgentParams.retry.composeLens(ActionRetryParams.capDelay).set(Some(v))(c)
      case WithTermination(v, c) => AgentParams.isTerminate.set(v)(c)
      case WithImportance(v, c)  => AgentParams.importance.set(v)(c)
      case WithSpans(v, c)       => AgentParams.spans.modify(_ ::: v)(c)
    }
}

final case class AgentConfig private (value: Fix[AgentConfigF]) {
  import AgentConfigF.*

  def withMaxRetries(num: Int): AgentConfig          = AgentConfig(Fix(WithMaxRetries(num, value)))
  def withCapDelay(dur: FiniteDuration): AgentConfig = AgentConfig(Fix(WithCapDelay(dur, value)))

  def withConstantDelay(delay: FiniteDuration): AgentConfig =
    AgentConfig(Fix(WithRetryPolicy(NJRetryPolicy.ConstantDelay(delay), value)))

  def withExponentialBackoff(delay: FiniteDuration): AgentConfig =
    AgentConfig(Fix(WithRetryPolicy(NJRetryPolicy.ExponentialBackoff(delay), value)))

  def withFibonacciBackoff(delay: FiniteDuration): AgentConfig =
    AgentConfig(Fix(WithRetryPolicy(NJRetryPolicy.FibonacciBackoff(delay), value)))

  def withFullJitterBackoff(delay: FiniteDuration): AgentConfig =
    AgentConfig(Fix(WithRetryPolicy(NJRetryPolicy.FullJitter(delay), value)))

  def withNonTermination: AgentConfig =
    AgentConfig(Fix(WithTermination(value = false, value)))

  def withLow: AgentConfig      = AgentConfig(Fix(WithImportance(Importance.Low, value)))
  def withMedium: AgentConfig   = AgentConfig(Fix(WithImportance(Importance.Medium, value)))
  def withHigh: AgentConfig     = AgentConfig(Fix(WithImportance(Importance.High, value)))
  def withCritical: AgentConfig = AgentConfig(Fix(WithImportance(Importance.Critical, value)))

  def withSpan(name: String): AgentConfig = AgentConfig(Fix(WithSpans(List(name), value)))

  def evalConfig: AgentParams = scheme.cata(algebra).apply(value)
}

private[guard] object AgentConfig {

  def apply(serviceParams: ServiceParams): AgentConfig =
    AgentConfig(Fix(AgentConfigF.InitParams[Fix[AgentConfigF]](serviceParams)))
}
