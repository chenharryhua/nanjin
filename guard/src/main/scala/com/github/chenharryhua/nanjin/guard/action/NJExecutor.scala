package com.github.chenharryhua.nanjin.guard.action

import cats.effect.Temporal
import cats.implicits.{toFlatMapOps, toFunctorOps}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.{Policy, TickStatus}

import scala.jdk.DurationConverters.JavaDurationOps

sealed trait NJExecutor[F[_]] {
  def retry[A](fa: F[A], worth: Throwable => Boolean): F[A]
  final def retry[A](fa: F[A]): F[A] = retry(fa, _ => true)
}

object NJExecutor {
  def dummy[F[_]]: NJExecutor[F] = new NJExecutor[F] {
    override def retry[A](fa: F[A], worth: Throwable => Boolean): F[A] = fa
  }

  private class Impl[F[_]](init: TickStatus)(implicit F: Temporal[F]) extends NJExecutor[F] {

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

  final class Builder private[guard] (isEnabled: Boolean, policy: Policy) extends EnableConfig[Builder] {

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, policy)

    def withPolicy(policy: Policy) =
      new Builder(isEnabled, policy)

    def build[F[_]: Temporal](init: TickStatus): NJExecutor[F] =
      if (isEnabled) new Impl[F](init.renewPolicy(policy)) else dummy[F]
  }
}
