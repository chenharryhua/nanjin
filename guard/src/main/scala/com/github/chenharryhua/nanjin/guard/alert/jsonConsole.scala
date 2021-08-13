package com.github.chenharryhua.nanjin.guard.alert

import cats.effect.std.Console
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import fs2.Pipe
import io.circe.syntax.*

object jsonConsole {

  def pipe[F[_]](implicit C: Console[F]): Pipe[F, NJEvent, Unit] = _.evalMap(event => C.println(event.asJson))
}
