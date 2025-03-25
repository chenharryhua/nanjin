package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.Sync
import cats.implicits.toFunctorOps

import java.time.{Instant, ZoneId}

final class TickStatus private (val tick: Tick, decisions: LazyList[PolicyF.CalcTick]) extends Serializable {

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
  def zeroth[F[_]: Sync](policy: Policy, zoneId: ZoneId): F[TickStatus] =
    Tick.zeroth[F](zoneId).map(new TickStatus(_, PolicyF.decisions(policy.policy)))
}
