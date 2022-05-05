package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Ref, Temporal}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.concurrent.Channel
import org.apache.commons.lang3.exception.ExceptionUtils
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
      us <- serviceStatus.updateAndGet(_.goDown(ts, retryDetails.upcomingDelay, ExceptionUtils.getMessage(ex)))
      uuid <- UUIDGen.randomUUID[F]
      _ <- channel.send(ServicePanic(us, ts, retryDetails, NJError(uuid, ex)))
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
