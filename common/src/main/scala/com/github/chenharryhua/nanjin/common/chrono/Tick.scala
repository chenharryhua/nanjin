package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.Sync
import cats.syntax.all.*
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.utils
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, HCursor, Json}
import monocle.macros.{Lenses, PLenses}
import org.typelevel.cats.time.instances.all.*

import java.time.*
import java.util.UUID

/*
 *  previous     acquire        wakeup
 *    |            |----snooze----|
 *    |---active---|              |
 *    |----------interval---------|
 */

@Lenses
final case class Tick(
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
  def zonedWakeup: ZonedDateTime = wakeup.atZone(zoneId)
  def zonedAcquire: ZonedDateTime = acquire.atZone(zoneId)
  def zonedPrevious: ZonedDateTime = previous.atZone(zoneId)

  // interval = active  +  snooze

  def interval: Duration = Duration.between(previous, wakeup)

  def active: Duration = Duration.between(previous, acquire)

  def sleepStretch(delay: Duration): Tick = copy(snooze = snooze.plus(delay))

  /** check if an instant is in this tick frame from previous timestamp(exclusive) to current
    * timestamp(inclusive).
    */
  def inBetween(now: Instant): Boolean =
    (now.isAfter(previous) && now.isBefore(wakeup)) || (now === wakeup)

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
    val id = show"$sequenceId".take(5)
    f"id=$id, idx=$index%04d, acq=$acq, wak=$wak, snz=$snz"
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

  def zeroth[F[_]: Sync](zoneId: ZoneId): F[Tick] =
    for {
      uuid <- utils.randomUUID[F]
      now <- Sync[F].realTimeInstant
    } yield zeroth(uuid, zoneId, now)

  implicit val encoderTick: Encoder[Tick] =
    (a: Tick) =>
      Json.obj(
        "sequence_id" -> a.sequenceId.asJson,
        "launch_time" -> a.launchTime.atZone(a.zoneId).toLocalDateTime.asJson,
        "zone_id" -> a.zoneId.asJson,
        "previous" -> a.zonedPrevious.toLocalDateTime.asJson,
        "acquire" -> a.zonedAcquire.toLocalDateTime.asJson,
        "wakeup" -> a.zonedWakeup.toLocalDateTime.asJson,
        "snooze" -> a.snooze.asJson,
        "index" -> Json.fromLong(a.index)
      )

  implicit val decoderTick: Decoder[Tick] =
    (c: HCursor) =>
      for {
        sequenceId <- c.get[UUID]("sequence_id")
        launchTime <- c.get[LocalDateTime]("launch_time")
        zoneId <- c.get[ZoneId]("zone_id")
        previous <- c.get[LocalDateTime]("previous")
        index <- c.get[Long]("index")
        acquire <- c.get[LocalDateTime]("acquire")
        snooze <- c.get[Duration]("snooze")
      } yield Tick(
        sequenceId = sequenceId,
        launchTime = launchTime.atZone(zoneId).toInstant,
        zoneId = zoneId,
        previous = previous.atZone(zoneId).toInstant,
        index = index,
        acquire = acquire.atZone(zoneId).toInstant,
        snooze = snooze
      )

  implicit val showTick: Show[Tick] = Show.fromToString[Tick]
}

@PLenses
final case class TickedValue[A](tick: Tick, value: A) {
  def map[B](f: A => B): TickedValue[B] = copy(value = f(value))
}
object TickedValue {
  implicit def encoderTickedValue[A: Encoder]: Encoder[TickedValue[A]] =
    io.circe.generic.semiauto.deriveEncoder[TickedValue[A]]

  implicit def decoderTickedValue[A: Decoder]: Decoder[TickedValue[A]] =
    io.circe.generic.semiauto.deriveDecoder[TickedValue[A]]

  implicit def showTickedValue[A: Show]: Show[TickedValue[A]] =
    cats.derived.semiauto.show[TickedValue[A]]

  implicit val functorTickedValue: Functor[TickedValue] = new Functor[TickedValue] {
    override def map[A, B](fa: TickedValue[A])(f: A => B): TickedValue[B] = fa.map(f)
  }
}
