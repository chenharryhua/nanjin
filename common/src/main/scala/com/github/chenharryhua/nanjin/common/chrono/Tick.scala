package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import cats.{Monad, Show}
import io.circe.generic.JsonCodec
import org.typelevel.cats.time.instances.all.*

import java.time.{Duration, Instant, ZoneId}
import java.util.UUID
import scala.annotation.tailrec

@JsonCodec
final case class Tick(
  sequenceId: UUID, // immutable
  launchTime: Instant, // immutable
  index: Long, // monotonously increase
  previous: Instant, // previous tick's wakeup time
  acquire: Instant, // when user acquire a new tick
  snooze: Duration // is/was snooze
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
      index = this.index + 1,
      previous = this.wakeup,
      acquire = now,
      snooze = delay
    )
}

object Tick {
  implicit val showTick: Show[Tick] = cats.derived.semiauto.show[Tick]
}

final class TickStatus private (val tick: Tick, decisions: LazyList[PolicyF.TickRequest => Option[Tick]])
    extends Serializable {

  def resetCounter: TickStatus =
    new TickStatus(tick, decisions)

  def withPolicy(policy: Policy, zoneId: ZoneId): TickStatus =
    new TickStatus(tick, PolicyF.decisions(policy.policy, zoneId))

  def next(now: Instant): Option[TickStatus] =
    decisions match {
      case head #:: tail => head(PolicyF.TickRequest(tick, now)).map(new TickStatus(_, tail))
      case _             => None
    }
}

object TickStatus {
  def apply[F[_]: Clock: UUIDGen: Monad](policy: Policy, zoneId: ZoneId): F[TickStatus] =
    for {
      uuid <- UUIDGen[F].randomUUID
      now <- Clock[F].realTimeInstant
    } yield {
      val zeroth = Tick(
        sequenceId = uuid,
        launchTime = now,
        index = 0L,
        previous = now,
        acquire = now,
        snooze = Duration.ZERO
      )
      new TickStatus(zeroth, PolicyF.decisions(policy.policy, zoneId))
    }
}
