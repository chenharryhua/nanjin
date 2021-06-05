package com.github.chenharryhua.nanjin.guard.alert

import cats.effect.Sync
import io.circe.syntax._

final private class ConsoleService[F[_]]()(implicit F: Sync[F]) extends AlertService[F] {
  override def alert(event: NJEvent): F[Unit] = F.blocking(println(event.asJson))
}

object ConsoleService {

  def apply[F[_]: Sync]: AlertService[F] = new ConsoleService[F]()
}
