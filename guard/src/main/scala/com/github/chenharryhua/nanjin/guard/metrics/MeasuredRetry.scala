package com.github.chenharryhua.nanjin.guard.metrics

import cats.effect.kernel.{Async, Outcome, Resource}
import cats.implicits.{catsSyntaxApplyOps, toFunctorOps}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.{Policy, TickStatus, TickedValue}
import com.github.chenharryhua.nanjin.guard.action.Retry

import java.time.ZoneId
import scala.concurrent.duration.FiniteDuration

object MeasuredRetry {

  final class Builder[F[_]] private[guard] (
    isEnabled: Boolean,
    policy: Policy,
    worthy: TickedValue[Throwable] => F[Boolean])
      extends EnableConfig[Builder[F]] {

    override def enable(isEnabled: Boolean): Builder[F] =
      new Builder[F](isEnabled, policy, worthy)

    def worthRetry(worthy: TickedValue[Throwable] => F[Boolean]): Builder[F] =
      new Builder[F](isEnabled, policy, worthy)

    def withPolicy(policy: Policy): Builder[F] =
      new Builder[F](isEnabled, policy, worthy)

    def withPolicy(f: Policy.type => Policy): Builder[F] =
      new Builder[F](isEnabled, f(Policy), worthy)

    private[guard] def build(mtx: Metrics[F], zoneId: ZoneId)(implicit F: Async[F]): Resource[F, Retry[F]] =
      if (isEnabled)
        for {
          failCounter <- mtx.permanentCounter("action_failed")
          cancelCounter <- mtx.permanentCounter("action_canceled")
          recentCounter <- mtx.counter("action_succeeded_recently")
          succeedTimer <- mtx.timer("action_succeeded")
          retry <- Resource.eval(TickStatus.zeroth[F](policy, zoneId)).map(ts => new Retry.Impl[F](ts))
        } yield new Retry[F] {
          override def apply[A](fa: => F[A]): F[A] =
            F.guaranteeCase[(FiniteDuration, A)](retry.comprehensive(F.timed(F.defer(fa)), worthy)) {
              case Outcome.Succeeded(ra) =>
                F.flatMap(ra)(a => succeedTimer.update(a._1) *> recentCounter.inc(1))
              case Outcome.Errored(_) => failCounter.inc(1)
              case Outcome.Canceled() => cancelCounter.inc(1)
            }.map(_._2)
        }
      else
        Resource.eval(TickStatus.zeroth[F](policy, zoneId)).map { ts =>
          val impl = new Retry.Impl[F](ts)
          new Retry[F] {
            override def apply[A](fa: => F[A]): F[A] = impl.comprehensive(F.defer(fa), worthy)
          }
        }
  }
}
