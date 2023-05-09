package com.github.chenharryhua.nanjin.http.client

import cats.effect.kernel.*
import cats.effect.{Async, Concurrent}
import cats.syntax.all.*
import fs2.compression.Compression
import natchez.Span
import org.http4s.client.Client
import org.http4s.client.middleware.*
import org.http4s.client.middleware.RetryPolicy.{exponentialBackoff, isErrorOrRetriableStatus}
import org.http4s.headers.`Set-Cookie`
import org.http4s.{Header, Headers, RequestCookie, Response}
import squants.information.{Information, Kilobytes}

import java.net.{CookieManager, CookieStore, HttpCookie, URI}
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

package object middleware {
  def exponentialRetry[F[_]: Temporal](maxWait: FiniteDuration, maxRetries: Int)(
    client: Client[F]): Client[F] =
    Retry[F](RetryPolicy[F](exponentialBackoff(maxWait, maxRetries), (_, r) => isErrorOrRetriableStatus(r)))(
      client)

  def logInsecurely[F[_]: Async](client: Client[F]): Client[F] =
    Logger(logHeaders = true, logBody = true, _ => false)(client)
  def logBoth[F[_]: Async](client: Client[F]): Client[F] = Logger(logHeaders = true, logBody = true)(client)
  def logHead[F[_]: Async](client: Client[F]): Client[F] = Logger(logHeaders = true, logBody = false)(client)
  def logBody[F[_]: Async](client: Client[F]): Client[F] = Logger(logHeaders = false, logBody = true)(client)

  def cookieJar[F[_]: Async](client: Client[F]): F[Client[F]] = CookieJar.impl[F](client)

  def gzip[F[_]: MonadCancelThrow: Compression](bufferSize: Information)(client: Client[F]): Client[F] =
    GZip[F](bufferSize.toBytes.toInt)(client)
  def gzip[F[_]: MonadCancelThrow: Compression](client: Client[F]): Client[F] =
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

  // steal from https://github.com/typelevel/natchez-http4s/blob/main/modules/http4s/src/main/scala/natchez/http4s/NatchezMiddleware.scala
  def traceClient[F[_]](parent: Span[F])(client: Client[F])(implicit
    ev: MonadCancel[F, Throwable]): Client[F] =
    Client { req =>
      parent.span("http4s-client-request").flatMap { span =>
        val cc: F[(Response[F], F[Unit])] = for {
          knl <- span.kernel
          _ <- span.put(
            "client_http_uri" -> req.uri.toString(),
            "client_http_method" -> req.method.toString
          )
          hs   = Headers(knl.toHeaders.map { case (k, v) => Header.Raw(k, v) }.toList)
          nReq = req.withHeaders(hs ++ req.headers)
          rsrc <- client.run(nReq).allocated
          _ <- span.put("client_http_status_code" -> rsrc._1.status.code.toString())
        } yield rsrc
        Resource(cc)
      }
    }
}
