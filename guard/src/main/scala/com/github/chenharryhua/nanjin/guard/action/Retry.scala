package com.github.chenharryhua.nanjin.guard.action

import cats.effect.Temporal
import cats.implicits.{toFlatMapOps, toFunctorOps}
import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickStatus}

import scala.jdk.DurationConverters.JavaDurationOps

trait SimpleRetry[F[_]] {
  def apply[A](fa: F[A]): F[A]
}

trait Retry[F[_]] extends SimpleRetry[F] {
  def apply[A](fa: F[A], worthy: (Tick, Throwable) => F[Boolean]): F[A]
}

object Retry {

  private[guard] class Impl[F[_]](initTS: TickStatus)(implicit F: Temporal[F]) extends Retry[F] {
    private[this] def comprehensive[A](fa: F[A], worthy: (Tick, Throwable) => F[Boolean]): F[A] =
      F.tailRecM[TickStatus, A](initTS) { status =>
        F.attempt(fa).flatMap {
          case Right(value) => F.pure(Right(value))
          case Left(ex) =>
            worthy(status.tick, ex).flatMap[Either[TickStatus, A]] {
              case false => F.raiseError(ex)
              case true =>
                for {
                  next <- F.realTimeInstant.map(status.next)
                  ns <- next match {
                    case None => F.raiseError(ex) // run out of policy
                    case Some(ts) => // sleep and continue
                      F.sleep(ts.tick.snooze.toScala).as(Left(ts))
                  }
                } yield ns
            }
        }
      }

    override def apply[A](fa: F[A]): F[A] =
      comprehensive(fa, (_, _) => F.pure(true))

    override def apply[A](fa: F[A], worthy: (Tick, Throwable) => F[Boolean]): F[A] =
      comprehensive(fa, worthy)
  }
}
