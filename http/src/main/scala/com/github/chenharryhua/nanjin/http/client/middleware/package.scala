package com.github.chenharryhua.nanjin.http.client

import cats.effect.kernel.Temporal
import cats.effect.{Async, Concurrent}
import org.http4s.client.Client
import org.http4s.client.middleware.*
import org.http4s.client.middleware.RetryPolicy.{exponentialBackoff, isErrorOrRetriableStatus}

import scala.concurrent.duration.FiniteDuration

package object middleware extends CookieBox {
  def exponentialRetry[F[_]: Temporal](maxWait: FiniteDuration, maxRetries: Int)(client: Client[F]): Client[F] =
    Retry[F](RetryPolicy[F](exponentialBackoff(maxWait, maxRetries), (_, r) => isErrorOrRetriableStatus(r)))(client)

  def logUnsecurely[F[_]: Async](client: Client[F]): Client[F] =
    Logger(logHeaders = true, logBody = true, _ => false)(client)
  def logBoth[F[_]: Async](client: Client[F]): Client[F] = Logger(logHeaders = true, logBody = true)(client)
  def logHead[F[_]: Async](client: Client[F]): Client[F] = Logger(logHeaders = true, logBody = false)(client)
  def logBody[F[_]: Async](client: Client[F]): Client[F] = Logger(logHeaders = false, logBody = true)(client)

  def cookieJar[F[_]: Async](client: Client[F]): F[Client[F]] = CookieJar.impl[F](client)

  def gzip[F[_]: Async](bufferSize: Int)(client: Client[F]): Client[F] = GZip[F](bufferSize)(client)

  def forwardRedirect[F[_]: Concurrent](maxRedirects: Int)(client: Client[F]): Client[F] =
    FollowRedirect[F](maxRedirects)(client)
}
