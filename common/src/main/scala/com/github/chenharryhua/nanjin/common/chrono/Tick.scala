package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.Sync
import cats.syntax.all.*
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.utils
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.typelevel.cats.time.instances.all.*

import java.time.*
import java.util.UUID

/*
 *  commence    acquires       conclude
 *    |            |----snooze----|
 *    |---active---|              |
 *    |----------interval---------|
 */

final case class Tick(
  sequenceId: UUID, // immutable
  launchTime: Instant, // immutable
  zoneId: ZoneId, // immutable
  index: Long, // monotonously increase
  commence: Instant,
  acquires: Instant,
  snooze: Duration
) {
  val conclude: Instant = acquires.plus(snooze)

  def zoned(f: this.type => Instant): ZonedDateTime = f(this).atZone(zoneId)
  def local(f: this.type => Instant): LocalDateTime = zoned(f).toLocalDateTime

  // interval = active  +  snooze

  def interval: Duration = Duration.between(commence, conclude)

  def active: Duration = Duration.between(commence, acquires)

  def snoozeStretch(delay: Duration): Tick = copy(snooze = snooze.plus(delay))

  /** check if an instant is in this tick frame from previous(exclusive) to wakeup(inclusive).
    */
  def isWithinOpenClosed(now: Instant): Boolean =
    (now.isAfter(commence) && now.isBefore(conclude)) || (now === conclude)

  /** check if an instant is in this tick frame from previous(inclusive) to wakeup (exclusive).
    */
  def isWithinClosedOpen(now: Instant): Boolean =
    (now.isAfter(commence) && now.isBefore(conclude)) || (now === commence)

  def nextTick(now: Instant, delay: Duration): Tick =
    copy(
      commence = this.conclude,
      index = this.index + 1,
      acquires = now,
      snooze = delay
    )

  override def toString: String = {
    val cls = local(_.conclude).show
    val acq = local(_.acquires).show
    val snz = snooze.show
    val id = show"$sequenceId".take(5)
    f"id=$id, idx=$index%04d, acq=$acq, cls=$cls, snz=$snz"
  }
}

object Tick {
  def zeroth(uuid: UUID, zoneId: ZoneId, now: Instant): Tick =
    Tick(
      sequenceId = uuid,
      launchTime = now,
      zoneId = zoneId,
      index = 0L,
      commence = now,
      acquires = now,
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
        "index" -> Json.fromLong(a.index),
        "commence" -> a.local(_.commence).asJson,
        "acquires" -> a.local(_.acquires).asJson,
        "conclude" -> a.local(_.conclude).asJson,
        "active" -> a.active.asJson,
        "snooze" -> a.snooze.asJson,
        "interval" -> a.interval.asJson,
        "sequence_id" -> a.sequenceId.asJson,
        "launch_time" -> a.local(_.launchTime).asJson,
        "zone_id" -> a.zoneId.asJson
      )

  implicit val decoderTick: Decoder[Tick] =
    (c: HCursor) =>
      for {
        sequenceId <- c.get[UUID]("sequence_id")
        launchTime <- c.get[LocalDateTime]("launch_time")
        zoneId <- c.get[ZoneId]("zone_id")
        index <- c.get[Long]("index")
        commence <- c.get[LocalDateTime]("commence")
        acquires <- c.get[LocalDateTime]("acquires")
        snooze <- c.get[Duration]("snooze")
      } yield Tick(
        sequenceId = sequenceId,
        launchTime = launchTime.atZone(zoneId).toInstant,
        zoneId = zoneId,
        index = index,
        commence = commence.atZone(zoneId).toInstant,
        acquires = acquires.atZone(zoneId).toInstant,
        snooze = snooze
      )

  implicit val showTick: Show[Tick] = Show.fromToString[Tick]
}

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
