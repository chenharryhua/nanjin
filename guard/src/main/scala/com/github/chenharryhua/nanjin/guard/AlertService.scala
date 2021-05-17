package com.github.chenharryhua.nanjin.guard

trait AlertService[F[_]] {
  def alert(msg: String): F[Unit]
}
