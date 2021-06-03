package com.github.chenharryhua.nanjin.guard.config

import cats.Functor
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import monocle.macros.Lenses

import scala.concurrent.duration._

@Lenses final case class GroupParams(maxRetries: Int, retryPolicy: NJRetryPolicy)

object GroupParams {

  def default: GroupParams = GroupParams(
    maxRetries = 3,
    retryPolicy = ConstantDelay(10.seconds)
  )
}

sealed trait GroupConfigF[A]

private object GroupConfigF {
  implicit val functorGroupConfigF: Functor[GroupConfigF] = cats.derived.semiauto.functor[GroupConfigF]

  final case class InitParams[K]() extends GroupConfigF[K]

  final case class WithMaxRetries[K](value: Int, cont: K) extends GroupConfigF[K]
  final case class WithRetryPolicy[K](value: NJRetryPolicy, cont: K) extends GroupConfigF[K]

  val algebra: Algebra[GroupConfigF, GroupParams] =
    Algebra[GroupConfigF, GroupParams] {
      case InitParams()          => GroupParams.default
      case WithRetryPolicy(v, c) => GroupParams.retryPolicy.set(v)(c)
      case WithMaxRetries(v, c)  => GroupParams.maxRetries.set(v)(c)
    }
}

final case class GroupConfig private (value: Fix[GroupConfigF]) {
  import GroupConfigF._
  def evalConfig: GroupParams               = scheme.cata(algebra).apply(value)
  def withMaxRetries(num: Int): GroupConfig = GroupConfig(Fix(WithMaxRetries(num, value)))

  def withConstantDelay(delay: FiniteDuration): GroupConfig =
    GroupConfig(Fix(WithRetryPolicy(ConstantDelay(delay), value)))

  def withExponentialBackoff(delay: FiniteDuration): GroupConfig =
    GroupConfig(Fix(WithRetryPolicy(ExponentialBackoff(delay), value)))

  def withFibonacciBackoff(delay: FiniteDuration): GroupConfig =
    GroupConfig(Fix(WithRetryPolicy(FibonacciBackoff(delay), value)))

  def withFullJitter(delay: FiniteDuration): GroupConfig =
    GroupConfig(Fix(WithRetryPolicy(FullJitter(delay), value)))

}

private[guard] object GroupConfig {
  val default: GroupConfig = new GroupConfig(Fix(GroupConfigF.InitParams[Fix[GroupConfigF]]()))
}
