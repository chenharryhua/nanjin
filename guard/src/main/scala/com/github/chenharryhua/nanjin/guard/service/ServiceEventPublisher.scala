package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Ref, Temporal}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.concurrent.Channel
import retry.RetryDetails

final private class ServiceEventPublisher[F[_]: UUIDGen](
  serviceStatus: Ref[F, ServiceStatus],
  channel: Channel[F, NJEvent])(implicit F: Temporal[F]) {

  def serviceReStart: F[Unit] =
    for {
      ss <- serviceStatus.get
      ts <- F.realTimeInstant.map(ss.serviceParams.toZonedDateTime)
      us <- serviceStatus.updateAndGet(_.goUp(ts))
      _ <- channel.send(ServiceStart(us, ts))
    } yield ()

  def servicePanic(retryDetails: RetryDetails, ex: Throwable): F[Unit] =
    for {
      ss <- serviceStatus.get
      ts <- F.realTimeInstant.map(ss.serviceParams.toZonedDateTime)
      err <- UUIDGen.randomUUID[F].map(NJError(_, ex))
      us <- serviceStatus.updateAndGet(_.goDown(ts, retryDetails.upcomingDelay, err.message))
      _ <- channel.send(ServicePanic(us, ts, retryDetails, err))
    } yield ()

  def serviceStop(cause: ServiceStopCause): F[Unit] =
    for {
      ss <- serviceStatus.get
      ts <- F.realTimeInstant.map(ss.serviceParams.toZonedDateTime)
      us <- serviceStatus.updateAndGet(_.goDown(ts, None, cause = cause.show))
      _ <- channel.send(
        ServiceStop(
          timestamp = ts,
          serviceStatus = us,
          cause = cause
        ))
    } yield ()
}
