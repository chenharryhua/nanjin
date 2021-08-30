package com.github.chenharryhua.nanjin.guard.observers

import cats.Applicative
import cats.data.Reader
import cats.effect.std.Console
import cats.implicits.{toFunctorOps, toShow}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import fs2.Chunk

object console {
  def apply[F[_]: Console: Applicative](f: NJEvent => String): NJConsole[F] =
    new NJConsole[F](Reader(f))
}

final class NJConsole[F[_]: Console: Applicative] private[observers] (converter: Reader[NJEvent, String])
    extends (NJEvent => F[Unit]) {

  override def apply(event: NJEvent): F[Unit] = Console[F].println(converter.run(event))
  def chunk(events: Chunk[NJEvent]): F[Unit]  = events.traverse(apply).void
}
