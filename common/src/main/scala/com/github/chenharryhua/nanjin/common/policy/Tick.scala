package com.github.chenharryhua.nanjin.common.policy

import cats.Monad
import cats.effect.kernel.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import io.circe.generic.JsonCodec

import java.time.{Duration, Instant}
import java.util.UUID

@JsonCodec
final case class Tick(
  id: UUID,
  start: Instant,
  index: Long,
  counter: Int,
  previous: Instant,
  snooze: Duration,
  acquire: Instant,
  next: Option[Instant]
) {
  val wakeup: Instant    = acquire.plus(snooze)
  val interval: Duration = Duration.between(previous, wakeup)
}

object Tick {
  def Zero[F[_]: Monad](implicit F: Clock[F], U: UUIDGen[F]): F[Tick] =
    for {
      uuid <- U.randomUUID
      now <- F.realTimeInstant
    } yield Tick(
      id = uuid,
      index = 0L,
      start = now,
      counter = 0,
      previous = now,
      snooze = Duration.ZERO,
      acquire = now,
      next = None
    )
}
