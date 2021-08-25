package com.github.chenharryhua.nanjin.guard.observers

import cats.data.Reader
import cats.effect.kernel.Sync
import cats.effect.std.Console
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import fs2.{INothing, Pipe, Stream}
import io.circe.syntax.EncoderOps

object console {
  private val default: EventFilter = EventFilter(
    serviceStart = true,
    servicePanic = true,
    serviceStop = true,
    actionSucc = true,
    actionRetry = true,
    actionFirstRetry = false,
    actionStart = true,
    actionFailure = true,
    fyi = true,
    passThrough = true,
    mrReport = true,
    sampling = 1
  )
  def json[F[_]: Sync: Console]: NJConsole[F] = new NJConsole[F](Reader(_.asJson.noSpaces), default)
  def text[F[_]: Sync: Console]: NJConsole[F] = new NJConsole[F](Reader(_.show), default)
}

final class NJConsole[F[_]](converter: Reader[NJEvent, String], eventFilter: EventFilter)(implicit
  F: Sync[F],
  C: Console[F])
    extends Pipe[F, NJEvent, INothing] {

  private def updateEventFilter(f: EventFilter => EventFilter): NJConsole[F] =
    new NJConsole[F](converter, f(eventFilter))

  def blockSucc: NJConsole[F]        = updateEventFilter(_.copy(actionSucc = false))
  def blockStart: NJConsole[F]       = updateEventFilter(_.copy(actionStart = false))
  def blockRetry: NJConsole[F]       = updateEventFilter(_.copy(actionRetry = false))
  def blockReport: NJConsole[F]      = updateEventFilter(_.copy(mrReport = false))
  def blockFyi: NJConsole[F]         = updateEventFilter(_.copy(fyi = false))
  def blockPassThrough: NJConsole[F] = updateEventFilter(_.copy(passThrough = false))

  override def apply(events: Stream[F, NJEvent]): Stream[F, INothing] =
    events.filter(eventFilter).evalMap(event => C.println(converter.run(event))).drain
}
