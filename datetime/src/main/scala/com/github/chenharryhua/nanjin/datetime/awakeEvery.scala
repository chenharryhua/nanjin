package com.github.chenharryhua.nanjin.datetime

import cats.effect.Temporal
import cats.syntax.all.*
import cats.{Order, Show}
import fs2.Stream
import io.circe.generic.JsonCodec
import org.typelevel.cats.time.instances.{duration, instant}
import retry.{PolicyDecision, RetryPolicy, RetryStatus}

import java.time.{Duration, Instant, ZoneId}
import scala.jdk.DurationConverters.{JavaDurationOps, ScalaDurationOps}

/** @param previous
  *   last wakeup time
  * @param wakeTime
  *   when sleep is over
  * @param snooze
  *   sleep duration of this tick
  */
@JsonCodec
final case class Tick(index: Int, snooze: Duration, previous: Instant, wakeTime: Instant) {
  lazy val interval: Duration = Duration.between(previous, wakeTime)
  def isNewDay(zoneId: ZoneId): Boolean =
    previous.atZone(zoneId).toLocalDate.isBefore(wakeTime.atZone(zoneId).toLocalDate)
}

object Tick extends duration with instant {
  implicit val orderingTick: Ordering[Tick] = Ordering.by(_.index)
  implicit val orderTick: Order[Tick]       = Order.fromOrdering[Tick]
  implicit val showTick: Show[Tick]         = cats.derived.semiauto.show[Tick]

  final def Zero: Tick = {
    val now: Instant = Instant.now()
    Tick(
      index = RetryStatus.NoRetriesYet.retriesSoFar,
      snooze = Duration.ZERO,
      previous = now,
      wakeTime = now
    )
  }
}
object awakeEvery {
  final private case class Status(status: RetryStatus, previous: Instant)

  final def apply[F[_]](policy: RetryPolicy[F])(implicit F: Temporal[F]): Stream[F, Tick] =
    Stream.eval(F.realTimeInstant).flatMap { kickoff =>
      Stream.unfoldEval[F, Status, Tick](Status(RetryStatus.NoRetriesYet, kickoff)) {
        case Status(status, previous) =>
          policy.decideNextRetry(status).flatMap[Option[(Tick, Status)]] {
            case PolicyDecision.GiveUp => F.pure(None)
            case PolicyDecision.DelayAndRetry(delay) =>
              F.sleep(delay) >> F.realTimeInstant.map { wakeup =>
                val rs: RetryStatus = status.addRetry(Duration.between(previous, wakeup).toScala)
                val tick: Tick =
                  Tick(
                    index = rs.retriesSoFar,
                    snooze = delay.toJava,
                    previous = previous,
                    wakeTime = wakeup
                  )
                (tick, Status(rs, wakeup)).some
              }
          }
      }
    }
}
