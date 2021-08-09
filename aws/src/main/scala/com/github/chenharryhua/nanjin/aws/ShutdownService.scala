package com.github.chenharryhua.nanjin.aws

private[aws] trait ShutdownService[F[_]] {
  def shutdown: F[Unit]
}
