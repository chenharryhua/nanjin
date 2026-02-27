package com.github.chenharryhua.nanjin.guard.metrics

trait Balance[F[_]] {
  def forward(num: Long): F[Unit]
  def backward(num: Long): F[Unit]
}
