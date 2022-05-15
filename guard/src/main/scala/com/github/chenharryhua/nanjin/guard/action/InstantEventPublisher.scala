package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Temporal
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.{Digested, Importance, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{InstantAlert, PassThrough}
import fs2.concurrent.Channel
import io.circe.Json

final private class InstantEventPublisher[F[_]](channel: Channel[F, NJEvent], serviceParams: ServiceParams)(implicit
  F: Temporal[F]) {
  def passThrough(metricName: Digested, json: Json, isError: Boolean): F[Unit] =
    for {
      ts <- F.realTimeInstant.map(serviceParams.toZonedDateTime)
      _ <- channel.send(
        PassThrough(
          metricName = metricName,
          timestamp = ts,
          serviceParams = serviceParams,
          isError = isError,
          value = json))
    } yield ()

  def alert(metricName: Digested, msg: String, importance: Importance): F[Unit] =
    for {
      ts <- F.realTimeInstant.map(serviceParams.toZonedDateTime)
      _ <- channel.send(
        InstantAlert(
          metricName = metricName,
          timestamp = ts,
          serviceParams = serviceParams,
          importance = importance,
          message = msg))
    } yield ()
}
