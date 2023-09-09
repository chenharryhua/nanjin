package com.github.chenharryhua.nanjin.datetime

import cats.effect.Temporal
import cats.effect.kernel.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import cats.{Monad, Order, Show}
import com.github.chenharryhua.nanjin.datetime.policies.Policy
import fs2.Stream
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import org.typelevel.cats.time.instances.instant.*
import retry.{PolicyDecision, RetryStatus}

import java.time.{Duration, Instant, ZoneId}
import java.util.UUID
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

object tickStream {

  // timeline |-----|----------|----------|------
  // ticks   zero   1          2          3
  //              first emit
  /** @param streamId
    *   id of the stream, unchanged throughout lifecycle
    * @param previous
    *   previous tick timestamp
    * @param timestamp
    *   emit time
    * @param predict
    *
    * Some(time): next tick comes after the time, roughly.
    *
    * None: next tick will never come
    * @param snoozed
    *   sleep duration of this tick
    */
  @JsonCodec
  final case class Tick(
    streamId: UUID,
    previous: Instant,
    timestamp: Instant,
    predict: Option[Instant],
    snoozed: Duration,
    status: RetryStatus) {
    val index: Int              = status.retriesSoFar
    lazy val interval: Duration = Duration.between(previous, timestamp)

    def isNewDay(zoneId: ZoneId): Boolean =
      previous.atZone(zoneId).toLocalDate.isBefore(timestamp.atZone(zoneId).toLocalDate)

    /** check if an instant is in this tick frame from previous timestamp(inclusive) to current
      * timestamp(exclusive).
      */
    def inBetween(now: Instant): Boolean =
      (now.isAfter(previous) || (now === previous)) && now.isBefore(timestamp)

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
        previous = now,
        timestamp = now,
        predict = None,
        snoozed = Duration.ZERO,
        status = RetryStatus.NoRetriesYet
      )
  }

  def apply[F[_]](policy: Policy[F], initTick: Tick)(implicit F: Temporal[F]): Stream[F, Tick] =
    Stream.unfoldEval[F, Tick, Tick](initTick) { previous =>
      policy.decideNextRetry(previous.status).flatMap[Option[(Tick, Tick)]] {
        case PolicyDecision.GiveUp => F.pure(None)
        case PolicyDecision.DelayAndRetry(delay) =>
          for {
            _ <- F.sleep(delay)
            wakeup <- F.realTimeInstant
            rs = previous.status.addRetry(Duration.between(previous.timestamp, wakeup).toScala)
            predict <- policy.decideNextRetry(rs).map {
              case PolicyDecision.GiveUp            => None
              case PolicyDecision.DelayAndRetry(nd) => Some(wakeup.plusNanos(nd.toNanos))
            }
          } yield {
            val tick: Tick =
              Tick(
                streamId = previous.streamId,
                previous = previous.timestamp,
                timestamp = wakeup,
                predict = predict,
                snoozed = delay.toJava,
                status = rs
              )
            (tick, tick).some
          }
      }
    }

  def apply[F[_]: UUIDGen: Temporal](policy: Policy[F]): Stream[F, Tick] =
    Stream.eval[F, Tick](Tick.Zero[F]).flatMap(apply(policy, _))
}
