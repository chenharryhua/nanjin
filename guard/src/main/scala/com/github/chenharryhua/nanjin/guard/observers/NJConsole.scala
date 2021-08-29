package com.github.chenharryhua.nanjin.guard.observers

import cats.data.Reader
import cats.effect.std.Console
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.guard.event.NJEvent

object console {
  def apply[F[_]: Console](f: NJEvent => String): NJConsole[F] =
    new NJConsole[F](Reader(f))
}

final class NJConsole[F[_]: Console] private[observers] (converter: Reader[NJEvent, String])
    extends (NJEvent => F[Unit]) {

  override def apply(event: NJEvent): F[Unit] = Console[F].println(converter.run(event))
}
