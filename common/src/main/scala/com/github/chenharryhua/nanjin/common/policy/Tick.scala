package com.github.chenharryhua.nanjin.common.policy

import cats.Monad
import cats.effect.kernel.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import io.circe.generic.JsonCodec
import org.typelevel.cats.time.instances.instant.*

import java.time.{Duration, Instant}
import java.util.UUID

@JsonCodec
final case class Tick(
  sequenceId: UUID,
  start: Instant,
  index: Long,
  counter: Int,
  previous: Instant,
  snooze: Duration,
  acquire: Instant,
  guessNext: Option[Instant]
) {
  val wakeup: Instant    = acquire.plus(snooze)
  val interval: Duration = Duration.between(previous, wakeup)

  /** check if an instant is in this tick frame from previous timestamp(inclusive) to current
    * timestamp(exclusive).
    */
  def inBetween(now: Instant): Boolean =
    (now.isAfter(previous) || (now === previous)) && now.isBefore(wakeup)
}

object Tick {
  def Zero[F[_]: Monad](implicit F: Clock[F], U: UUIDGen[F]): F[Tick] =
    for {
      uuid <- U.randomUUID
      now <- F.realTimeInstant
    } yield Tick(
      sequenceId = uuid,
      index = 0L,
      start = now,
      counter = 0,
      previous = now,
      snooze = Duration.ZERO,
      acquire = now,
      guessNext = None
    )

  def unsafeZero: Tick = {
    val now = Instant.now()
    Tick(
      sequenceId = UUID.randomUUID(),
      index = 0L,
      start = now,
      counter = 0,
      previous = now,
      snooze = Duration.ZERO,
      acquire = now,
      guessNext = None
    )
  }
}
