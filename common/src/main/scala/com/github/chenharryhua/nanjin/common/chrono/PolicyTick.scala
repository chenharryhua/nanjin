package com.github.chenharryhua.nanjin.common.chrono

import cats.Monad
import cats.effect.kernel.{Clock, Sync}
import cats.effect.std.{Random, SecureRandom}
import cats.syntax.applicative.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.option.none

import java.time.{Instant, ZoneId}

/** PolicyTick wraps a Tick with a sequence of policy-driven decisions. next() computes the next tick
  * according to the policy.
  */
final class PolicyTick[F[_]: {Random, Monad}] private (val tick: Tick, decisions: LazyList[TickStepper[F]]) {

  def renewPolicy(policy: Policy): PolicyTick[F] =
    new PolicyTick(tick, EvalPolicy(policy.policy))

  def withTick(tick: Tick): PolicyTick[F] =
    new PolicyTick(tick, decisions)

  def next(now: Instant): F[Option[PolicyTick[F]]] =
    decisions match {
      case head #:: tail =>
        if (now.isBefore(tick.conclude)) { // pretend it concludes on time
          head.step(tick, tick.conclude).map(tk => Some(new PolicyTick(tk, tail)))
        } else
          head.step(tick, now).map(tk => Some(new PolicyTick(tk, tail)))

      case _ => none[PolicyTick[F]].pure[F]
    }

  def advance(using C: Clock[F]): F[Option[PolicyTick[F]]] =
    C.realTimeInstant.flatMap(next)
}

object PolicyTick {
  def zeroth[F[_]: Sync](zoneId: ZoneId, policy: Policy): F[PolicyTick[F]] =
    SecureRandom.javaSecuritySecureRandom[F].flatMap { sr =>
      given dummy: SecureRandom[F] = sr
      Tick.zeroth[F](zoneId).map(new PolicyTick(_, EvalPolicy(policy.policy)))
    }

  def apply[F[_]: Sync](tick: Tick): F[PolicyTick[F]] =
    SecureRandom.javaSecuritySecureRandom[F].map { sr =>
      given dummy: SecureRandom[F] = sr
      new PolicyTick[F](tick, LazyList.empty)
    }
}
