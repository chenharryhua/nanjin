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
final class PolicyTick[F[_]: {Random, Monad}] private (val tick: Tick, steppers: LazyList[TickStepper[F]]) {

  def renewPolicy(policy: Policy): PolicyTick[F] =
    new PolicyTick(tick, EvalPolicy(policy.policy))

  def withTick(tick: Tick): PolicyTick[F] =
    new PolicyTick(tick, steppers)

  def next(now: Instant): F[Option[PolicyTick[F]]] =
    steppers match {
      case head #:: tail =>
        if (now.isBefore(tick.conclude))
          head.step(tick, tick.conclude).map(_.map(tk => new PolicyTick(tk, tail)))
        else
          head.step(tick, now).map(_.map(tk => new PolicyTick(tk, tail)))
      case _ => none[PolicyTick[F]].pure[F]
    }

  def advance(using C: Clock[F]): F[Option[PolicyTick[F]]] =
    C.realTimeInstant.flatMap(next)
}

object PolicyTick {

  /** One `SecureRandom` serves both uses here: the jitter source the steppers draw from, and the sequence id
    * of the seed tick.
    *
    * It replaces two generators. `SecureRandom.of` builds one instance per processor to spread contention,
    * which a single sequential tick stream never encounters, and `Tick.seed[F](zoneId)` would then build a
    * second of its own. Sharing one instance keeps both uses cryptographically sourced, and leaves nothing to
    * gain from drawing jitter out of a cheaper non-crypto generator, since the sequence id would still need
    * this one.
    *
    * This removes surplus allocation rather than a measured bottleneck. Seeding is dominated by other work
    * and the end-to-end difference did not rise above the noise of ad-hoc timing, so treat it as tidier
    * rather than faster.
    */
  def seed[F[_]](zoneId: ZoneId, policy: Policy)(using F: Sync[F]): F[PolicyTick[F]] =
    SecureRandom.javaSecuritySecureRandom[F].flatMap { implicit sr =>
      Tick.seed[F](zoneId, sr).map(new PolicyTick(_, EvalPolicy(policy.policy)))
    }
}
