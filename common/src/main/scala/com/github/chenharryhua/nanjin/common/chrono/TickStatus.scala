package com.github.chenharryhua.nanjin.common.chrono

import cats.MonadError
import cats.effect.kernel.Sync
import cats.effect.std.Random
import cats.implicits.{catsSyntaxApplicativeId, none, showInterpolator, toFlatMapOps, toFunctorOps}
import org.typelevel.cats.time.instances.instant

import java.time.{Instant, ZoneId}

/** TickStatus wraps a Tick with a sequence of policy-driven decisions. next() computes the next tick
  * according to the policy.
  */
final class TickStatus[F[_]: Random] private (val tick: Tick, decisions: LazyList[PolicyF.CalcTick[F]])(
  implicit F: MonadError[F, Throwable])
    extends instant {

  def renewPolicy(policy: Policy): TickStatus[F] =
    new TickStatus(tick, PolicyF.evaluatePolicy(policy.policy))

  def withTick(tick: Tick): TickStatus[F] =
    new TickStatus(tick, decisions)

  def next(now: Instant): F[Option[TickStatus[F]]] =
    decisions match {
      case head #:: tail =>
        if (!now.isBefore(tick.conclude))
          head(PolicyF.TickRequest(tick, now)).map(r => Some(new TickStatus(r, tail)))
        else
          F.raiseError(
            new IllegalArgumentException(
              show"Invalid time: now=$now is before tick.conclude=${tick.conclude}"))

      case _ => none[TickStatus[F]].pure[F]
    }
}

object TickStatus {
  def zeroth[F[_]: Sync](zoneId: ZoneId, policy: Policy): F[TickStatus[F]] =
    Random.scalaUtilRandom[F].flatMap { implicit rnd =>
      Tick.zeroth[F](zoneId).map(new TickStatus(_, PolicyF.evaluatePolicy(policy.policy)))
    }

  def apply[F[_]: Sync](tick: Tick): F[TickStatus[F]] =
    Random.scalaUtilRandom[F].map { implicit rng =>
      new TickStatus[F](tick, LazyList.empty)
    }
}
