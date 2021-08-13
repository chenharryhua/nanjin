package com.github.chenharryhua.nanjin.guard.alert

import cats.effect.std.Console
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import io.circe.syntax.*

final private class ConsoleService[F[_]](implicit C: Console[F]) extends AlertService[F] {
  override def alert(event: NJEvent): F[Unit] = C.println(event.asJson)
}

object ConsoleService {

  def apply[F[_]: Console]: AlertService[F] = new ConsoleService[F]
}
