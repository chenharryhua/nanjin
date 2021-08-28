package com.github.chenharryhua.nanjin.guard.observers

import cats.data.Reader
import cats.effect.std.Console
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import fs2.{INothing, Pipe, Stream}

object console {
  def apply[F[_]: Console](f: NJEvent => String): NJConsole[F] =
    new NJConsole[F](Reader(f))
}

final class NJConsole[F[_]: Console] private[observers] (converter: Reader[NJEvent, String])
    extends Pipe[F, NJEvent, INothing] {

  override def apply(events: Stream[F, NJEvent]): Stream[F, INothing] =
    events.evalMap(event => Console[F].println(converter.run(event))).drain
}
