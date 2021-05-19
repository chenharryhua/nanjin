package com.github.chenharryhua.nanjin.guard

import cats.effect.Async

trait AlertService[F[_]] {
  def alert(status: Status)(implicit F: Async[F]): F[Unit]
}
