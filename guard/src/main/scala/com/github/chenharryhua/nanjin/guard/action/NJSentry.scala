package com.github.chenharryhua.nanjin.guard.action

import cats.effect.Temporal
import cats.implicits.{toFlatMapOps, toFunctorOps}
import com.github.chenharryhua.nanjin.common.chrono.TickStatus

import scala.jdk.DurationConverters.JavaDurationOps

sealed trait NJSentry[F[_]] {
  def retry[A](fa: F[A], worth: Throwable => Boolean): F[A]
  final def retry[A](fa: F[A]): F[A] = retry(fa, _ => true)
}

object NJSentry {
  def dummy[F[_]]: NJSentry[F] = new NJSentry[F] {
    override def retry[A](fa: F[A], worth: Throwable => Boolean): F[A] = fa
  }

  private class Impl[F[_]](init: TickStatus)(implicit F: Temporal[F]) extends NJSentry[F] {

    override def retry[A](fa: F[A], worth: Throwable => Boolean): F[A] =
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

  def apply[F[_]: Temporal](init: TickStatus): NJSentry[F] = new Impl[F](init)
}
