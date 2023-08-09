package com.github.chenharryhua.nanjin.datetime

import cats.effect.Temporal
import cats.effect.kernel.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import cats.{Monad, Order, Show}
import fs2.Stream
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import retry.{PolicyDecision, RetryPolicy, RetryStatus}

import java.time.{Duration, Instant, ZoneId}
import java.util.UUID
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

object tickStream {

  /** @param streamId
    *   unchanged throughout lifecycle
    * @param previous
    *   last wakeup time
    * @param wakeTime
    *   when sleep is over
    * @param snooze
    *   sleep duration of this tick
    */
  @JsonCodec
  final case class Tick private[tickStream] (
    streamId: UUID,
    snooze: Duration,
    previous: Instant,
    wakeTime: Instant,
    status: RetryStatus) {
    val index: Int              = status.retriesSoFar
    lazy val interval: Duration = Duration.between(previous, wakeTime)
    def isNewDay(zoneId: ZoneId): Boolean =
      previous.atZone(zoneId).toLocalDate.isBefore(wakeTime.atZone(zoneId).toLocalDate)

    def inBetween(now: Instant): Boolean =
      now.isAfter(previous) && now.isBefore(wakeTime)
  }

  object Tick extends DateTimeInstances {
    implicit val orderingTick: Ordering[Tick] = Ordering.by(_.index)
    implicit val orderTick: Order[Tick]       = Order.fromOrdering[Tick]
    implicit val showTick: Show[Tick]         = cats.derived.semiauto.show[Tick]

    def Zero[F[_]: Monad](implicit F: Clock[F], U: UUIDGen[F]): F[Tick] =
      for {
        uuid <- U.randomUUID
        now <- F.realTimeInstant
      } yield Tick(
        streamId = uuid,
        snooze = Duration.ZERO,
        previous = now,
        wakeTime = now,
        status = RetryStatus.NoRetriesYet
      )
  }

  def apply[F[_]](policy: RetryPolicy[F], initTick: Tick)(implicit F: Temporal[F]): Stream[F, Tick] =
    Stream.unfoldEval[F, Tick, Tick](initTick) { previous =>
      policy.decideNextRetry(previous.status).flatMap[Option[(Tick, Tick)]] {
        case PolicyDecision.GiveUp => F.pure(None)
        case PolicyDecision.DelayAndRetry(delay) =>
          F.sleep(delay) >> F.realTimeInstant.map { wakeup =>
            val tick: Tick =
              Tick(
                streamId = previous.streamId,
                snooze = delay.toJava,
                previous = previous.wakeTime,
                wakeTime = wakeup,
                status = previous.status.addRetry(Duration.between(previous.wakeTime, wakeup).toScala)
              )
            (tick, tick).some
          }
      }
    }

  def apply[F[_]: UUIDGen: Temporal](policy: RetryPolicy[F]): Stream[F, Tick] =
    Stream.eval[F, Tick](Tick.Zero[F]).flatMap(apply(policy, _))
}
