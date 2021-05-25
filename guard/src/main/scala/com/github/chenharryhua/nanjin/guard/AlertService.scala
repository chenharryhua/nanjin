package com.github.chenharryhua.nanjin.guard

trait AlertService[F[_]] {
  def alert(status: Status): F[Unit]
}
