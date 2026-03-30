package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.{Async, Ref, Resource}
import cats.syntax.flatMap.given
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy}

import java.time.ZoneId

package object metrics {
  private[metrics] def run_gauge_job_background[F[_], A](
    fa: F[A],
    zoneId: ZoneId,
    policy: Policy.type => Policy)(using F: Async[F]): Resource[F, Ref[F, A]] =
    for {
      init <- Resource.eval(fa)
      ref <- Resource.eval(F.ref(init))
      _ <- F.background(
        tickStream.tickScheduled[F](zoneId, policy).evalMap(_ => fa.flatMap(ref.set)).compile.drain)
    } yield ref
}
