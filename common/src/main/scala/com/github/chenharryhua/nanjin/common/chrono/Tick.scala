package com.github.chenharryhua.nanjin.common.chrono

import cats.Monad
import cats.effect.kernel.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import io.circe.generic.JsonCodec
import org.typelevel.cats.time.instances.all.*

import java.time.{Duration, Instant, ZoneId}
import java.util.UUID

@JsonCodec
final case class Tick(
  sequenceId: UUID, // immutable
  launchTime: Instant, // immutable
  zoneId: ZoneId, // immutable
  previous: Instant, // previous tick's wakeup time
  index: Long, // monotonously increase
  acquire: Instant, // when user acquire a new tick
  snooze: Duration // sleep duration
) {
  val wakeup: Instant    = acquire.plus(snooze)
  def interval: Duration = Duration.between(previous, wakeup)

  /** check if an instant is in this tick frame from previous timestamp(inclusive) to current
    * timestamp(exclusive).
    */
  def inBetween(now: Instant): Boolean =
    (now.isAfter(previous) || (now === previous)) && now.isBefore(wakeup)

  def newTick(now: Instant, delay: Duration): Tick =
    copy(
      previous = this.wakeup,
      index = this.index + 1,
      acquire = now,
      snooze = delay
    )
}

final class TickStatus private (val tick: Tick, decisions: LazyList[PolicyF.CalcTick]) extends Serializable {

  def renewPolicy(policy: Policy): TickStatus =
    new TickStatus(tick, PolicyF.decisions(policy.policy, tick.zoneId))

  def next(now: Instant): Option[TickStatus] =
    decisions match {
      case head #:: tail => head(PolicyF.TickRequest(tick, now)).map(new TickStatus(_, tail))
      case _             => None
    }
}

object TickStatus {
  def apply(tick: Tick): TickStatus = new TickStatus(tick, LazyList.empty)
  def zeroth[F[_]: Clock: UUIDGen: Monad](policy: Policy, zoneId: ZoneId): F[TickStatus] =
    for {
      uuid <- UUIDGen[F].randomUUID
      now <- Clock[F].realTimeInstant
    } yield {
      val zeroth = Tick(
        sequenceId = uuid,
        launchTime = now,
        zoneId = zoneId,
        previous = now,
        index = 0L,
        acquire = now,
        snooze = Duration.ZERO
      )
      new TickStatus(zeroth, PolicyF.decisions(policy.policy, zoneId))
    }
}
