package com.github.chenharryhua.nanjin.http.client.middleware

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.eq.*
import org.http4s.RequestCookie
import org.http4s.client.Client
import org.http4s.headers.`Set-Cookie`

import java.net.{CookieManager, CookieStore, HttpCookie, URI}
import scala.collection.JavaConverters.*

/** use [[org.http4s.client.middleware.CookieJar]] whenever it works.
  */
private[middleware] trait CookieBox {

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
