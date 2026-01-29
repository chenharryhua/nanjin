package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.Sync
import cats.implicits.toFunctorOps

import java.time.{Instant, ZoneId}

/** TickStatus wraps a Tick with a sequence of policy-driven decisions. next() computes the next tick
  * according to the policy.
  */
final class TickStatus private (val tick: Tick, decisions: LazyList[PolicyF.CalcTick]) {

  def renewPolicy(policy: Policy): TickStatus =
    new TickStatus(tick, PolicyF.decisions(policy.policy))

  def next(now: Instant): Option[TickStatus] =
    decisions match {
      case head #:: tail => Some(new TickStatus(head(PolicyF.TickRequest(tick, now)), tail))
      case _             => None
    }
}

object TickStatus {
  def apply(tick: Tick): TickStatus = new TickStatus(tick, LazyList.empty)
  def zeroth[F[_]: Sync](zoneId: ZoneId, policy: Policy): F[TickStatus] =
    Tick.zeroth[F](zoneId).map(new TickStatus(_, PolicyF.decisions(policy.policy)))
}
