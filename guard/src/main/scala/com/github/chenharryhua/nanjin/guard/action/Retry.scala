package com.github.chenharryhua.nanjin.guard.action

import cats.effect.Temporal
import cats.implicits.{toFlatMapOps, toFunctorOps}
import com.github.chenharryhua.nanjin.common.chrono.{Tick, TickStatus}

import scala.jdk.DurationConverters.JavaDurationOps

sealed trait Retry[F[_]] {
  def apply[A](arrow: (Tick, Option[Throwable]) => F[Either[Throwable, A]]): F[A]
  def apply[A](tfa: Tick => F[A]): F[A]
  def apply[A](fa: F[A]): F[A]
}

object Retry {

  private[guard] class Impl[F[_]](initTS: TickStatus)(implicit F: Temporal[F]) extends Retry[F] {

    private[this] def comprehensive[A](arrow: (Tick, Option[Throwable]) => F[Either[Throwable, A]]): F[A] =
      F.tailRecM[(TickStatus, Option[Throwable]), A](initTS -> None) { case (status, lastError) =>
        F.attempt(arrow(status.tick, lastError)).flatMap[Either[(TickStatus, Option[Throwable]), A]] {
          case Right(errorOrValue) => // stop retry
            errorOrValue match {
              case Left(userError) => F.raiseError(userError) // quit with user provided error
              case Right(value)    => F.pure(Right(value)) // quit with value
            }
          case Left(ex) => // retrying
            for {
              next <- F.realTimeInstant.map(status.next)
              ns <- next match {
                case None => F.raiseError(ex) // run out of policy
                case Some(ts) => // sleep and continue
                  F.sleep(ts.tick.snooze.toScala).as(Left(ts -> Some(ex)))
              }
            } yield ns
        }
      }

    override def apply[A](fa: F[A]): F[A] =
      comprehensive((_: Tick, _: Option[Throwable]) => fa.map(Right(_)))

    override def apply[A](tfa: Tick => F[A]): F[A] =
      comprehensive((tick: Tick, _: Option[Throwable]) => tfa(tick).map(Right(_)))

    override def apply[A](arrow: (Tick, Option[Throwable]) => F[Either[Throwable, A]]): F[A] =
      comprehensive(arrow)
  }
}
