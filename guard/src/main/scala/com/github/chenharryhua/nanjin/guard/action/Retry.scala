package com.github.chenharryhua.nanjin.guard.action

import cats.effect.Temporal
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Hotswap
import cats.implicits.{toFlatMapOps, toFunctorOps}
import com.github.chenharryhua.nanjin.common.chrono.{Policy, TickStatus, TickedValue}

import java.time.ZoneId
import scala.jdk.DurationConverters.JavaDurationOps

trait Retry[F[_]] {
  def apply[A](fa: F[A]): F[A]
  def apply[A](rfa: Resource[F, A]): Resource[F, A]
}

object Retry {

  final private class Impl[F[_]](initTS: TickStatus)(implicit F: Temporal[F]) {

    def comprehensive[A](fa: F[A], worthy: TickedValue[Throwable] => F[Boolean]): F[A] =
      F.tailRecM[TickStatus, A](initTS) { status =>
        F.handleErrorWith(fa.map[Either[TickStatus, A]](Right(_))) { ex =>
          F.flatMap[Boolean, Either[TickStatus, A]](worthy(TickedValue(status.tick, ex))) {
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

    def resource[A](rfa: Resource[F, A], worthy: TickedValue[Throwable] => F[Boolean]): Resource[F, A] =
      Hotswap.create[F, A].evalMap(hotswap => comprehensive(hotswap.swap(rfa), worthy))
  }

  final class Builder[F[_]] private[guard] (policy: Policy, worthy: TickedValue[Throwable] => F[Boolean]) {

    def isWorthRetry(worthy: TickedValue[Throwable] => F[Boolean]): Builder[F] =
      new Builder[F](policy, worthy)

    def withPolicy(policy: Policy): Builder[F] =
      new Builder[F](policy, worthy)

    def withPolicy(f: Policy.type => Policy): Builder[F] =
      new Builder[F](f(Policy), worthy)

    private[guard] def build(zoneId: ZoneId)(implicit F: Async[F]): Resource[F, Retry[F]] =
      Resource.eval(TickStatus.zeroth[F](policy, zoneId)).map { ts =>
        val impl = new Impl[F](ts)
        new Retry[F] {
          override def apply[A](fa: F[A]): F[A] =
            impl.comprehensive(fa, worthy)

          override def apply[A](rfa: Resource[F, A]): Resource[F, A] =
            impl.resource(rfa, worthy)
        }
      }
  }
}
