package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Clock, Ref}
import cats.effect.kernel.Resource.ExitCase
import cats.syntax.all.*
import cats.Monad
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, ServiceStopCause}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ServiceStart, ServiceStop}
import fs2.Chunk

import java.util.UUID

final private class FinalizeMonitor[F[_]: Clock: Monad, A](
  translate: NJEvent => F[Option[A]],
  ref: Ref[F, Map[UUID, ServiceStart]]) {
  def monitoring(event: NJEvent): F[Unit] = event match {
    case ss: ServiceStart => ref.update(_.updated(ss.serviceParams.serviceId.value, ss))
    case ss: ServiceStop  => ref.update(_.removed(ss.serviceParams.serviceId.value))
    case _                => Monad[F].unit
  }

  def terminated(ec: ExitCase): F[Chunk[A]] = for {
    ts <- Clock[F].realTimeInstant
    msgs <- ref.get.flatMap(m =>
      Chunk
        .iterable(m.values)
        .traverseFilter(ss =>
          translate(
            ServiceStop(ss.serviceParams, ss.serviceParams.toZonedDateTime(ts), ServiceStopCause(ec)))))
  } yield msgs
}
