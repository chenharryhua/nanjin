package com.github.chenharryhua.nanjin.guard.metrics

import cats.effect.Temporal
import cats.effect.kernel.{Async, Outcome, Resource}
import cats.implicits.{catsSyntaxApplyOps, toFlatMapOps, toFunctorOps}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.{Policy, TickStatus, TickedValue}

import java.time.ZoneId
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.JavaDurationOps

trait Retry[F[_]] {
  def apply[A](fa: F[A]): F[A]
}

object Retry {

  private class Impl[F[_]](initTS: TickStatus)(implicit F: Temporal[F]) {

    def comprehensive[A](fa: F[A], worthy: TickedValue[Throwable] => F[Boolean]): F[A] =
      F.tailRecM[TickStatus, A](initTS) { status =>
        F.attempt(fa).flatMap {
          case Right(value) => F.pure(Right(value))
          case Left(ex) =>
            worthy(TickedValue(status.tick, ex)).flatMap[Either[TickStatus, A]] {
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
  }

  final class Builder[F[_]] private[guard] (
    isEnabled: Boolean,
    policy: Policy,
    callback: TickedValue[Throwable] => F[Boolean])
      extends EnableConfig[Builder[F]] {

    override def enable(isEnabled: Boolean): Builder[F] =
      new Builder[F](isEnabled, policy, callback)

    def worthRetry(f: TickedValue[Throwable] => F[Boolean]): Builder[F] =
      new Builder[F](isEnabled, policy, f)

    def withPolicy(policy: Policy): Builder[F] =
      new Builder[F](isEnabled, policy, callback)

    def withPolicy(f: Policy.type => Policy): Builder[F] =
      new Builder[F](isEnabled, f(Policy), callback)

    private[guard] def build(mtx: Metrics[F], zoneId: ZoneId)(implicit F: Async[F]): Resource[F, Retry[F]] =
      for {
        failCounter <- mtx.permanentCounter("failed", _.enable(isEnabled))
        cancelCounter <- mtx.permanentCounter("canceled", _.enable(isEnabled))
        recentCounter <- mtx.counter("recent", _.enable(isEnabled))
        succeedTimer <- mtx.timer("succeeded", _.enable(isEnabled))
        retry <- Resource.eval(TickStatus.zeroth[F](policy, zoneId)).map(ts => new Retry.Impl[F](ts))
      } yield new Retry[F] {
        override def apply[A](fa: F[A]): F[A] =
          F.guaranteeCase[(FiniteDuration, A)](retry.comprehensive(F.timed(fa), callback)) {
            case Outcome.Succeeded(ra) =>
              F.flatMap(ra)(a => succeedTimer.update(a._1) *> recentCounter.inc(1))
            case Outcome.Errored(_) => failCounter.inc(1)
            case Outcome.Canceled() => cancelCounter.inc(1)
          }.map(_._2)
      }
  }
}
