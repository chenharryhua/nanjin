package com.github.chenharryhua.nanjin.common.chrono

import cats.derived.derived
import cats.effect.kernel.Sync
import cats.effect.std.{SecureRandom, UUIDGen}
import cats.syntax.eq.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.show.{showInterpolator, given}
import cats.{Functor, Show}
import com.github.chenharryhua.nanjin.common.DurationFormatter
import fs2.timeseries.TimeStamped
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.typelevel.cats.time.instances.duration.durationInstances
import org.typelevel.cats.time.instances.instant.instantInstances
import org.typelevel.cats.time.instances.localdatetime.localdatetimeInstances

import java.time.{Duration, Instant, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

/** A `Tick` represents a bounded, evolving **time-frame** with identity and structure.
  *
  * A tick captures how time is partitioned into three ordered instants:
  *
  * {{{
  *   commence   ──►  acquires  ──►  conclude
  *       |              |--- snooze ---|
  *       |--- active ---|
  *       |----------- window ----------|
  * }}}
  *
  * Conceptually:
  *
  *   - `commence` marks the start of the time-frame
  *   - `acquires` marks the point at which the tick is acquired
  *   - `conclude` marks the end of the time-frame
  *
  * in sleep-then-emit mode, conclude represents the **moment the downstream actually receives the tick
  *
  * in emit-then-sleep mode, acquires represents the **moment the downstream actually receives the tick
  *
  * A `Tick` carries stable identity and temporal provenance, allowing it to be:
  *
  *   - evolved deterministically (`nextTick`)
  *   - measured (`active`, `snooze`, `window`)
  *   - serialized and reconstructed across systems
  *
  * ===Invariants===
  *
  * The following invariants are assumed to hold:
  *
  *   - `launchTime <= commence <= acquires <= conclude`
  *   - `index` is monotonically increasing within a `sequenceId`
  *   - `sequenceId`, `launchTime`, and `zoneId` are immutable across evolution
  *
  * These invariants are not enforced at runtime and must be preserved by constructors and transformation
  * methods.
  *
  * ===Time semantics===
  *
  * All internal fields are stored as `Instant`. Local and zoned representations are derived using the
  * associated `zoneId`.
  *
  * A `Tick` is independent of any particular domain (e.g. retry, scheduling, polling) and may be used
  * wherever a structured time-frame is required.
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

  /** Stretch the conclude time by adding a delay */
  def withSnoozeStretch(delay: Duration): Tick = copy(conclude = conclude.plus(delay))

  /** Replace the conclude time entirely */
  def withConclude(newConclude: Instant): Tick = copy(conclude = newConclude)

  /** check if an instant is in this tick frame from commence(exclusive) to conclude(inclusive).
    */
  def isWithinOpenClosed(now: Instant): Boolean =
    (now.isAfter(commence) && now.isBefore(conclude)) || (now === conclude)

  /** check if an instant is in this tick frame from commence(inclusive) to conclude(exclusive).
    */
  def isWithinClosedOpen(now: Instant): Boolean =
    (now.isAfter(commence) && now.isBefore(conclude)) || (now === commence)

  def nextTick(acquires: Instant, conclude: Instant): Tick =
    copy(
      commence = this.conclude,
      index = this.index + 1,
      acquires = acquires,
      conclude = conclude
    )

  override def toString: String = {
    val cld = local(_.conclude).show.take(24).padTo(24, ' ')
    val acq = local(_.acquires).show.take(24).padTo(24, ' ')
    val snz = snooze.show.take(9).padTo(9, ' ')
    val id = show"$sequenceId".take(3)
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

  def zeroth[F[_]](zoneId: ZoneId)(using F: Sync[F]): F[Tick] =
    SecureRandom.javaSecuritySecureRandom[F].flatMap { sr =>
      UUIDGen.fromSecureRandom[F](using F, sr).randomUUID.flatMap { uuid =>
        F.realTimeInstant.map { now =>
          zeroth(uuid, zoneId, now)
        }
      }
    }

  private val fmt = DurationFormatter.defaultFormatter

  given Encoder[Tick] =
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

  given Decoder[Tick] =
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

  given Show[Tick] = Show.fromToString[Tick]
}

/** A value annotated with the `Tick` (time-frame) in which it was produced or observed.
  *
  * This type preserves temporal provenance alongside a computed value, allowing downstream consumers to make
  * decisions with full time-frame context.
  *
  * Common use cases include:
  *   - time-aware decision-making
  *   - observability and metrics
  *   - state transitions with temporal bounds
  *
  * `TickedValue` forms a lawful `Functor`, mapping over the value while preserving the associated `Tick`.
  */
final case class TickedValue[A](tick: Tick, value: A) derives Show, Encoder, Decoder:
  def map[B](f: A => B): TickedValue[B] = copy(value = f(value))

  def withSnoozeStretch(delay: Duration): TickedValue[A] =
    copy(tick = tick.withSnoozeStretch(delay))

  def withConclude(newConclude: Instant): TickedValue[A] =
    copy(tick = tick.withConclude(newConclude))

  def withNextTick(acquires: Instant, conclude: Instant): TickedValue[A] =
    TickedValue(tick.nextTick(acquires, conclude), value)

  def resolveTime(f: Tick => FiniteDuration): TimeStamped[A] =
    TimeStamped[A](f(tick), value)

object TickedValue:
  given Functor[TickedValue] with
    override def map[A, B](fa: TickedValue[A])(f: A => B): TickedValue[B] = fa.map(f)
end TickedValue
