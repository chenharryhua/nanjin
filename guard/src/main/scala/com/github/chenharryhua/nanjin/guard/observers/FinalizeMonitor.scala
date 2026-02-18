package com.github.chenharryhua.nanjin.guard.observers

import cats.Monad
import cats.effect.kernel.{Clock, Ref}
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import com.github.chenharryhua.nanjin.guard.config.ServiceId
import com.github.chenharryhua.nanjin.guard.event.Event.{ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.{Event, ServiceStopCause}
import fs2.Chunk
import com.github.chenharryhua.nanjin.guard.event.Timestamp

final private class FinalizeMonitor[F[_]: Clock: Monad, A](
  translate: Event => F[Option[A]],
  ref: Ref[F, Map[ServiceId, ServiceStart]]) {
  def monitoring(event: Event): F[Unit] = event match {
    case ss: ServiceStart => ref.update(_.updated(ss.serviceParams.serviceId, ss))
    case ss: ServiceStop  => ref.update(_.removed(ss.serviceParams.serviceId))
    case _                => ().pure[F]
  }

  val terminated: F[Chunk[A]] = for {
    ts <- Clock[F].realTimeInstant
    messages <- ref
      .modify(m => Map.empty[ServiceId, ServiceStart] -> m.values)
      .flatMap(values =>
        Chunk
          .from(values)
          .traverseFilter(ss =>
            translate(
              ServiceStop(
                ss.serviceParams,
                Timestamp(ss.serviceParams.toZonedDateTime(ts)),
                ServiceStopCause.ByCancellation))))
  } yield messages
}
