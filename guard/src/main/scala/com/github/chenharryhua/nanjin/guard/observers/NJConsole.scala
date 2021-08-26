package com.github.chenharryhua.nanjin.guard.observers

import cats.data.Reader
import cats.effect.std.Console
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import fs2.{INothing, Pipe, Stream}

object console {
  def apply[F[_]: Console](f: NJEvent => String): NJConsole[F] =
    new NJConsole[F](Reader(f), EventFilter.all)
}

final class NJConsole[F[_]: Console] private[observers] (converter: Reader[NJEvent, String], eventFilter: EventFilter)
    extends Pipe[F, NJEvent, INothing] {

  private def updateEventFilter(f: EventFilter => EventFilter): NJConsole[F] =
    new NJConsole[F](converter, f(eventFilter))

  def blockSucc: NJConsole[F]        = updateEventFilter(EventFilter.actionSucced.set(false))
  def blockStart: NJConsole[F]       = updateEventFilter(EventFilter.actionStart.set(false))
  def blockFyi: NJConsole[F]         = updateEventFilter(EventFilter.fyi.set(false))
  def blockPassThrough: NJConsole[F] = updateEventFilter(EventFilter.passThrough.set(false))

  override def apply(events: Stream[F, NJEvent]): Stream[F, INothing] =
    events.filter(eventFilter).evalMap(event => Console[F].println(converter.run(event))).drain
}
