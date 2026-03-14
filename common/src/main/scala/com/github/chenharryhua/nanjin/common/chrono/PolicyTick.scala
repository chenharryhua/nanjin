package com.github.chenharryhua.nanjin.common.chrono

import cats.Monad
import cats.effect.kernel.{Clock, Sync}
import cats.effect.std.{Random, SecureRandom}
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.option.none

import java.time.{Instant, ZoneId}

/** PolicyTick wraps a Tick with a sequence of policy-driven decisions. next() computes the next tick
  * according to the policy.
  */
final class PolicyTick[F[_]: {Random, Monad}] private (
  val tick: Tick,
  decisions: LazyList[PolicyF.CalcTick[F]]) {

  def renewPolicy(policy: Policy): PolicyTick[F] =
    new PolicyTick(tick, PolicyF.evaluatePolicy(policy.policy))

  def withTick(tick: Tick): PolicyTick[F] =
    new PolicyTick(tick, decisions)

  def next(now: Instant): F[Option[PolicyTick[F]]] =
    decisions match {
      case head #:: tail =>
        if (now.isBefore(tick.conclude)) { // pretend it concludes on time
          head(PolicyF.TickRequest(tick, tick.conclude)).map(tk => Some(new PolicyTick(tk, tail)))
        } else
          head(PolicyF.TickRequest(tick, now)).map(tk => Some(new PolicyTick(tk, tail)))

      case _ => none[PolicyTick[F]].pure[F]
    }

  def advance(using C: Clock[F]): F[Option[PolicyTick[F]]] =
    C.realTimeInstant.flatMap(next)
}

object PolicyTick {
  def zeroth[F[_]: Sync](zoneId: ZoneId, policy: Policy): F[PolicyTick[F]] =
    SecureRandom.javaSecuritySecureRandom[F].flatMap { sr =>
      given dummy: SecureRandom[F] = sr
      Tick.zeroth[F](zoneId).map(new PolicyTick(_, PolicyF.evaluatePolicy(policy.policy)))
    }

  def apply[F[_]: Sync](tick: Tick): F[PolicyTick[F]] =
    SecureRandom.javaSecuritySecureRandom[F].map { sr =>
      given dummy: SecureRandom[F] = sr
      new PolicyTick[F](tick, LazyList.empty)
    }
}
