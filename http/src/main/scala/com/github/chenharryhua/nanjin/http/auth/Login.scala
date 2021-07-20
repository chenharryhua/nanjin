package com.github.chenharryhua.nanjin.http.auth

import cats.effect.Async
import fs2.Stream
import org.http4s.client.Client

trait Login[F[_]] {
  def login(client: Client[F])(implicit F: Async[F]): Stream[F, Client[F]]
}
