package com.github.chenharryhua.nanjin.guard

import cats.Functor
import cats.effect.Temporal
import cats.effect.kernel.Clock
import cats.syntax.all.*
import fs2.Stream
import io.circe.generic.JsonCodec
import retry.{PolicyDecision, RetryPolicy, RetryStatus}

import java.time.{Duration, Instant}
import scala.jdk.DurationConverters.ScalaDurationOps

/** @param pullTime
  *   when it is pulled
  * @param wakeTime
  *   when sleep is over
  * @param delay
  *   nominal delay
  */
@JsonCodec
final case class Tick(index: Int, pullTime: Instant, wakeTime: Instant, delay: Duration)

object Tick {
  final def Zero[F[_]: Clock: Functor]: F[Tick] = Clock[F].realTimeInstant.map { now =>
    Tick(
      index = RetryStatus.NoRetriesYet.retriesSoFar,
      pullTime = now,
      wakeTime = now,
      delay = Duration.ZERO
    )
  }
}

object awakeEvery {
  def apply[F[_]](policy: RetryPolicy[F])(implicit F: Temporal[F]): Stream[F, Tick] =
    Stream.unfoldEval[F, RetryStatus, Tick](RetryStatus.NoRetriesYet) { status =>
      for {
        start <- F.realTimeInstant
        next <- policy.decideNextRetry(status).flatMap[Option[(Tick, RetryStatus)]] {
          case PolicyDecision.GiveUp => F.pure(None)
          case PolicyDecision.DelayAndRetry(delay) =>
            F.sleep(delay) >> F.realTimeInstant.map { wakeup =>
              val rs: RetryStatus = status.addRetry(delay)
              val tick: Tick =
                Tick(
                  index = rs.retriesSoFar,
                  pullTime = start,
                  wakeTime = wakeup,
                  delay = delay.toJava
                )
              ((tick, rs)).some
            }
        }
      } yield next
    }
}
