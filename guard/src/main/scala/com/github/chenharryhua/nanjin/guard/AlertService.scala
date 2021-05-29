package com.github.chenharryhua.nanjin.guard

trait AlertService[F[_]] {
  def alert(event: NJEvent): F[Unit]
}
