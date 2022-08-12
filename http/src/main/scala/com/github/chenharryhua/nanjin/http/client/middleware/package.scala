package com.github.chenharryhua.nanjin.http.client

import cats.effect.kernel.{Resource, Sync, Temporal}
import cats.effect.{Async, Concurrent}
import cats.syntax.eq.*
import org.http4s.RequestCookie
import org.http4s.client.Client
import org.http4s.client.middleware.*
import org.http4s.client.middleware.RetryPolicy.{exponentialBackoff, isErrorOrRetriableStatus}
import org.http4s.headers.`Set-Cookie`
import squants.information.{Information, Kilobytes}

import java.net.{CookieManager, CookieStore, HttpCookie, URI}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

package object middleware {
  def exponentialRetry[F[_]: Temporal](maxWait: FiniteDuration, maxRetries: Int)(
    client: Client[F]): Client[F] =
    Retry[F](RetryPolicy[F](exponentialBackoff(maxWait, maxRetries), (_, r) => isErrorOrRetriableStatus(r)))(
      client)

  def logUnsecurely[F[_]: Async](client: Client[F]): Client[F] =
    Logger(logHeaders = true, logBody = true, _ => false)(client)
  def logBoth[F[_]: Async](client: Client[F]): Client[F] = Logger(logHeaders = true, logBody = true)(client)
  def logHead[F[_]: Async](client: Client[F]): Client[F] = Logger(logHeaders = true, logBody = false)(client)
  def logBody[F[_]: Async](client: Client[F]): Client[F] = Logger(logHeaders = false, logBody = true)(client)

  def cookieJar[F[_]: Async](client: Client[F]): F[Client[F]] = CookieJar.impl[F](client)

  def gzip[F[_]: Async](bufferSize: Information)(client: Client[F]): Client[F] =
    GZip[F](bufferSize.toBytes.toInt)(client)
  def gzip[F[_]: Async](client: Client[F]): Client[F] =
    GZip[F](Kilobytes(32).toBytes.toInt)(client)

  def forwardRedirect[F[_]: Concurrent](maxRedirects: Int)(client: Client[F]): Client[F] =
    FollowRedirect[F](maxRedirects)(client)

  def cookieBox[F[_]](cookieManager: CookieManager)(client: Client[F])(implicit F: Sync[F]): Client[F] = {
    val cookieStore: CookieStore = cookieManager.getCookieStore
    Client[F] { req =>
      for {
        cookies <- Resource.eval(
          F.delay(
            cookieStore
              .get(URI.create(req.uri.renderString))
              .asScala
              .toList
              .map(hc => RequestCookie(hc.getName, hc.getValue))))
        out <- client.run(cookies.foldLeft(req) { case (r, c) => r.addCookie(c) })
      } yield {
        out.headers.headers
          .filter(_.name === `Set-Cookie`.name)
          .flatMap(c => HttpCookie.parse(c.value).asScala)
          .foreach(hc => cookieStore.add(URI.create(req.uri.renderString), hc))
        out
      }
    }
  }
}
