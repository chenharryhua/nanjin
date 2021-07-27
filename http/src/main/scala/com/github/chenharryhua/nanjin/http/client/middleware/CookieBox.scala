package com.github.chenharryhua.nanjin.http.client.middleware

import cats.effect.{Async, Resource}
import org.http4s.RequestCookie
import org.http4s.client.Client
import org.typelevel.ci.CIString

import java.net.{CookieManager, CookieStore, HttpCookie, URI}
import scala.collection.JavaConverters.*

/** use [[org.http4s.client.middleware.CookieJar]] whenever it works.
  */
private[middleware] trait CookieBox {

  def cookieBox[F[_]](cookieManager: CookieManager)(client: Client[F])(implicit F: Async[F]): Client[F] = {
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
          .filter(_.name == CIString("Set-Cookie"))
          .flatMap(c => HttpCookie.parse(c.value).asScala)
          .foreach(hc => cookieStore.add(URI.create(req.uri.renderString), hc))
        out
      }
    }
  }
}
