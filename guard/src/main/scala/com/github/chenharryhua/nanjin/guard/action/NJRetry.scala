package com.github.chenharryhua.nanjin.guard.action

import cats.effect.Temporal
import cats.implicits.{toFlatMapOps, toFunctorOps}
import com.github.chenharryhua.nanjin.common.chrono.TickStatus

import scala.jdk.DurationConverters.JavaDurationOps

sealed trait NJRetry[F[_]] {
  def apply[A](fa: F[A], worth: Throwable => Boolean): F[A]
  final def apply[A](fa: F[A]): F[A] = apply(fa, _ => true)
}

object NJRetry {

  private[guard] class Impl[F[_]](init: TickStatus)(implicit F: Temporal[F]) extends NJRetry[F] {

    override def apply[A](fa: F[A], worth: Throwable => Boolean): F[A] =
      F.tailRecM(init) { status =>
        F.attempt(fa).flatMap[Either[TickStatus, A]] {
          case Right(out) => F.pure(Right(out))
          case Left(ex) =>
            if (worth(ex)) {
              for {
                next <- F.realTimeInstant.map(status.next)
                tickStatus <- next match {
                  case None => F.raiseError(ex) // run out of policy
                  case Some(ts) => // sleep and continue
                    F.sleep(ts.tick.snooze.toScala).as(Left(ts))
                }
              } yield tickStatus
            } else {
              F.raiseError(ex) // unworthy retry
            }
        }
      }
  }
}
