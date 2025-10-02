package com.github.chenharryhua.nanjin.http.client.middleware
import cats.effect.kernel.*
import cats.syntax.all.*
import org.http4s.RequestCookie
import org.http4s.client.Client
import org.http4s.headers.`Set-Cookie`

import java.net.{CookieManager, CookieStore, HttpCookie, URI}
import scala.jdk.CollectionConverters.*

object cookieBox {
  def apply[F[_]: MonadCancelThrow](cookieManager: CookieManager)(client: Client[F]): Client[F] = {
    val cookie_store: CookieStore = cookieManager.getCookieStore
    Client[F] { req =>
      for {
        cookies <- Resource.pure[F, List[RequestCookie]](
          cookie_store
            .get(URI.create(req.uri.renderString))
            .asScala
            .toList
            .map(hc => RequestCookie(hc.getName, hc.getValue)))
        out <- client.run(cookies.foldLeft(req) { case (r, c) => r.addCookie(c) })
      } yield {
        out.headers.headers
          .filter(_.name === `Set-Cookie`.name)
          .flatMap(c => HttpCookie.parse(c.value).asScala)
          .foreach(hc => cookie_store.add(URI.create(req.uri.renderString), hc))
        out
      }
    }
  }
}
