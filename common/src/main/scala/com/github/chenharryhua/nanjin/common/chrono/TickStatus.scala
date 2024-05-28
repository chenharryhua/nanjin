package com.github.chenharryhua.nanjin.common.chrono

import cats.Monad
import cats.effect.kernel.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*

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
  def zeroth[F[_]: Clock: UUIDGen: Monad](policy: Policy, zoneId: ZoneId): F[TickStatus] =
    Tick.zeroth[F](zoneId).map(new TickStatus(_, PolicyF.decisions(policy.policy)))
}
