package com.github.chenharryhua.nanjin.common.chrono

import cats.effect.kernel.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import cats.{Monad, Show}
import io.circe.generic.JsonCodec
import org.typelevel.cats.time.instances.all.*

import java.time.{Duration, Instant}
import java.util.UUID

@JsonCodec
final case class Tick(
  sequenceId: UUID, // immutable
  launchTime: Instant, // immutable
  index: Long, // monotonously increase
  counter: Int,
  previous: Instant, // previous tick's wakeup time
  acquire: Instant,
  snooze: Duration
) {
  val wakeup: Instant    = acquire.plus(snooze)
  val interval: Duration = Duration.between(previous, wakeup)

  /** check if an instant is in this tick frame from previous timestamp(inclusive) to current
    * timestamp(exclusive).
    */
  def inBetween(now: Instant): Boolean =
    (now.isAfter(previous) || (now === previous)) && now.isBefore(wakeup)

  def newTick(now: Instant, delay: Duration): Tick =
    copy(
      index = this.index + 1,
      counter = this.counter + 1,
      previous = this.wakeup,
      acquire = now,
      snooze = delay
    )
}

object Tick {
  implicit val showTick: Show[Tick] = cats.derived.semiauto.show[Tick]

  def Zero[F[_]: Monad](implicit F: Clock[F], U: UUIDGen[F]): F[Tick] =
    for {
      uuid <- U.randomUUID
      now <- F.realTimeInstant
    } yield Tick(
      sequenceId = uuid,
      launchTime = now,
      index = 0L,
      counter = 0,
      previous = now,
      acquire = now,
      snooze = Duration.ZERO
    )
}
