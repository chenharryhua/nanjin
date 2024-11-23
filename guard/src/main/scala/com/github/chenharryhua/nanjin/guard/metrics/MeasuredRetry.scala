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
        failCounter <- mtx.permanentCounter("action_failed", _.enable(isEnabled))
        cancelCounter <- mtx.permanentCounter("action_canceled", _.enable(isEnabled))
        recentCounter <- mtx.counter("action_recently", _.enable(isEnabled))
        succeedTimer <- mtx.timer("action_succeeded", _.enable(isEnabled))
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
