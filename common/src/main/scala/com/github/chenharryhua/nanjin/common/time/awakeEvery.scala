package com.github.chenharryhua.nanjin.common.time

import cats.effect.Temporal
import cats.syntax.all.*
import cats.{Order, Show}
import fs2.Stream
import io.circe.generic.JsonCodec
import org.typelevel.cats.time.instances.{duration, instant}
import retry.{PolicyDecision, RetryPolicy, RetryStatus}

import java.time.{Duration, Instant}
import scala.jdk.DurationConverters.ScalaDurationOps

/** @param pullTime
  *   when it is pulled
  * @param wakeTime
  *   when sleep is over
  * @param delay
  *   nominal delay, actual delay is equal to wakeTime minus pullTime roughly
  */
@JsonCodec
final case class Tick(index: Int, pullTime: Instant, wakeTime: Instant, delay: Duration)

object Tick extends duration with instant {
  implicit val orderingTick: Ordering[Tick] = Ordering.by(_.index)
  implicit val orderTick: Order[Tick]       = Order.fromOrdering[Tick]
  implicit val showTick: Show[Tick]         = cats.derived.semiauto.show[Tick]

  final val Zero: Tick = Tick(
    index = RetryStatus.NoRetriesYet.retriesSoFar,
    pullTime = Instant.ofEpochMilli(0),
    wakeTime = Instant.ofEpochMilli(0),
    delay = Duration.ZERO
  )
}
object awakeEvery {
  final def apply[F[_]](policy: RetryPolicy[F])(implicit F: Temporal[F]): Stream[F, Tick] =
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
              (tick, rs).some
            }
        }
      } yield next
    }
}
