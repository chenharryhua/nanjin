package com.github.chenharryhua.nanjin.guard

import cats.effect.Sync

trait AlertService[F[_]] {
  def alert(status: Status)(implicit F: Sync[F]): F[Unit]
}
