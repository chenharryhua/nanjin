package com.github.chenharryhua.nanjin.guard.sinks

import cats.effect.std.Console
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import fs2.{INothing, Pipe}
import io.circe.syntax.*

object jsonConsole {

  def sink[F[_]](implicit C: Console[F]): Pipe[F, NJEvent, INothing] = _.evalMap(event => C.println(event.asJson)).drain
}
