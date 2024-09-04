package com.github.chenharryhua.nanjin.common.chrono

import cats.Monad
import cats.effect.kernel.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import io.circe.generic.JsonCodec
import org.typelevel.cats.time.instances.all.*

import java.time.{Duration, Instant, ZoneId, ZonedDateTime}
import java.util.UUID

@JsonCodec
final case class Tick private[chrono] (
  sequenceId: UUID, // immutable
  launchTime: Instant, // immutable
  zoneId: ZoneId, // immutable
  previous: Instant, // previous tick's wakeup time
  index: Long, // monotonously increase
  acquire: Instant, // when acquire a new tick
  snooze: Duration // sleep duration
) {

  val wakeup: Instant = acquire.plus(snooze)

  def zonedLaunchTime: ZonedDateTime = launchTime.atZone(zoneId)
  def zonedWakeup: ZonedDateTime     = wakeup.atZone(zoneId)
  def zonedAcquire: ZonedDateTime    = acquire.atZone(zoneId)
  def zonedPrevious: ZonedDateTime   = previous.atZone(zoneId)

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

  override def toString: String = {
    val wak = zonedWakeup.toLocalDateTime.show
    val acq = zonedAcquire.toLocalDateTime.show
    val snz = snooze.show
    f"idx=$index%04d, acq=$acq, wak=$wak, snz=$snz"
  }
}

object Tick {
  def zeroth(uuid: UUID, zoneId: ZoneId, now: Instant): Tick =
    Tick(
      sequenceId = uuid,
      launchTime = now,
      zoneId = zoneId,
      previous = now,
      index = 0L,
      acquire = now,
      snooze = Duration.ZERO
    )

  def zeroth[F[_]: Clock: UUIDGen: Monad](zoneId: ZoneId): F[Tick] =
    for {
      uuid <- UUIDGen[F].randomUUID
      now <- Clock[F].realTimeInstant
    } yield zeroth(uuid, zoneId, now)
}
