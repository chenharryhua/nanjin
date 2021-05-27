package com.github.chenharryhua.nanjin.guard

trait AlertService[F[_]] {
  def alert(event: Event): F[Unit]
}
