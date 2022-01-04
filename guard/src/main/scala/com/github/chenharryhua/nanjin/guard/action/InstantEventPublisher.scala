package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Temporal
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.{DigestedName, Importance, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{InstantAlert, NJEvent, PassThrough}
import fs2.concurrent.Channel
import io.circe.Json

final private class InstantEventPublisher[F[_]](channel: Channel[F, NJEvent], serviceParams: ServiceParams)(implicit
  F: Temporal[F]) {
  def passThrough(metricName: DigestedName, json: Json, asError: Boolean): F[Unit] =
    for {
      ts <- F.realTimeInstant
      _ <- channel.send(
        PassThrough(
          metricName = metricName,
          asError = asError,
          timestamp = ts,
          serviceParams = serviceParams,
          value = json))
    } yield ()

  def alert(metricName: DigestedName, msg: String, importance: Importance): F[Unit] =
    for {
      ts <- F.realTimeInstant
      _ <- channel.send(
        InstantAlert(
          metricName = metricName,
          timestamp = ts,
          importance = importance,
          serviceParams = serviceParams,
          message = msg))
    } yield ()
}
