package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.Sync
import cats.syntax.all.*
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.{utils, DurationFormatter}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.typelevel.cats.time.instances.all.*

import java.time.*
import java.util.UUID

/*
 *  commence    acquires       conclude
 *    |            |----snooze----|
 *    |---active---|              |
 *    |----------window-----------|
 */

final case class Tick(
  sequenceId: UUID, // immutable
  launchTime: Instant, // immutable
  zoneId: ZoneId, // immutable
  index: Long, // monotonously increase
  commence: Instant,
  acquires: Instant,
  conclude: Instant
) {
  val snooze: Duration = Duration.between(acquires, conclude)

  def zoned(f: this.type => Instant): ZonedDateTime = f(this).atZone(zoneId)
  def local(f: this.type => Instant): LocalDateTime = zoned(f).toLocalDateTime

  // window = active  +  snooze

  def window: Duration = Duration.between(commence, conclude)

  def active: Duration = Duration.between(commence, acquires)

  def withSnoozeStretch(delay: Duration): Tick = copy(conclude = conclude.plus(delay))

  /** check if an instant is in this tick frame from commence(exclusive) to conclude(inclusive).
    */
  def isWithinOpenClosed(now: Instant): Boolean =
    (now.isAfter(commence) && now.isBefore(conclude)) || (now === conclude)

  /** check if an instant is in this tick frame from commence(inclusive) to conclude(exclusive).
    */
  def isWithinClosedOpen(now: Instant): Boolean =
    (now.isAfter(commence) && now.isBefore(conclude)) || (now === commence)

  def nextTick(now: Instant, wakeup: Instant): Tick =
    copy(
      commence = this.conclude,
      index = this.index + 1,
      acquires = now,
      conclude = wakeup
    )

  override def toString: String = {
    val cld = local(_.conclude).show
    val acq = local(_.acquires).show
    val snz = snooze.show
    val id = show"$sequenceId".take(5)
    f"id=$id, idx=$index%04d, acq=$acq, cld=$cld, snz=$snz"
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
      conclude = now
    )

  def zeroth[F[_]: Sync](zoneId: ZoneId): F[Tick] =
    for {
      uuid <- utils.randomUUID[F]
      now <- Sync[F].realTimeInstant
    } yield zeroth(uuid, zoneId, now)

  private val fmt = DurationFormatter.defaultFormatter

  implicit val encoderTick: Encoder[Tick] =
    (a: Tick) =>
      Json.obj(
        "index" -> Json.fromLong(a.index),
        "commence" -> a.local(_.commence).asJson,
        "acquires" -> a.local(_.acquires).asJson,
        "conclude" -> a.local(_.conclude).asJson,
        "active" -> fmt.format(a.active).asJson,
        "snooze" -> fmt.format(a.snooze).asJson,
        "window" -> fmt.format(a.window).asJson,
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
        conclude <- c.get[LocalDateTime]("conclude")
      } yield Tick(
        sequenceId = sequenceId,
        launchTime = launchTime.atZone(zoneId).toInstant,
        zoneId = zoneId,
        index = index,
        commence = commence.atZone(zoneId).toInstant,
        acquires = acquires.atZone(zoneId).toInstant,
        conclude = conclude.atZone(zoneId).toInstant
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
