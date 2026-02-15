package com.github.chenharryhua.nanjin.http.client.middleware
import cats.effect.kernel.*
import cats.syntax.eq.catsSyntaxEq
import org.http4s.RequestCookie
import org.http4s.client.Client
import org.http4s.headers.`Set-Cookie`

import java.net.{CookieManager, CookieStore, HttpCookie, URI}
import scala.jdk.CollectionConverters.*

/** `cookieBox` is an HTTP client middleware that provides automatic cookie management using a provided
  * `java.net.CookieManager`.
  *
  * It does the following:
  *
  *   1. **Reads cookies** from the `CookieManager` before sending the request
  *      - Extracts cookies for the request URI
  *      - Converts them to `RequestCookie` and adds them to the outgoing request
  *   2. **Writes cookies** to the `CookieManager` after receiving the response
  *      - Parses all `Set-Cookie` headers from the response
  *      - Converts them to `HttpCookie` and adds them to the `CookieStore` for future requests
  *   3. **Blocking operations** (Java cookie store read/write) are shifted to `Sync[F].blocking` to avoid
  *      blocking the Cats Effect compute pool.
  *   4. **Resource-safe**
  *      - The middleware preserves the `Resource` lifecycle of the wrapped client
  *      - Response finalizers are not affected
  *   5. **Thread Safety**
  *      - The built-in Java SE `CookieManager` and its default `CookieStore` are designed to be thread-safe.
  *        Concurrent requests can safely read/write cookies without additional synchronization.
  *      - Note: Heavy contention in extremely concurrent scenarios may cause minor performance bottlenecks
  *        due to internal synchronization of the store.
  *
  * **Usage Example**
  * {{{
  *   import cats.effect.IO
  *   import org.http4s.client.Client
  *   import java.net.CookieManager
  *
  *   val cookieManager = new CookieManager()
  *   val baseClient: Client[IO] = ???
  *
  *   val clientWithCookies: Client[IO] = cookieBox[IO](cookieManager)(baseClient)
  *
  *   clientWithCookies.run(Request[IO](uri = uri"https://example.com")).use { resp =>
  *     IO(println(resp.status))
  *   }
  * }}}
  *
  * **Notes**
  *   - Cookies are keyed per URI; multiple cookies for the same URI are supported.
  *   - CE3 fibers calling the middleware concurrently are safe, thanks to Java SE synchronization.
  */
object cookieBox {

  /** cookieManager the underlying Java CookieManager that holds cookies
    * @param cookieManager
    *   java CookieManager
    * @param client
    *   the HTTP client to wrap
    * @tparam F
    *   the effect type, must have `Sync[F]` to safely handle blocking Java calls
    * @return
    *   a new `Client[F]` that automatically manages cookies
    */
  def apply[F[_]](cookieManager: CookieManager)(client: Client[F])(implicit F: Sync[F]): Client[F] = {
    val cookie_store: CookieStore = cookieManager.getCookieStore
    Client[F] { req =>
      for {
        cookies <- Resource.eval[F, List[RequestCookie]](
          F.blocking(
            cookie_store
              .get(URI.create(req.uri.renderString))
              .asScala
              .toList
              .map(hc => RequestCookie(hc.getName, hc.getValue))))
        out <- client.run(cookies.foldLeft(req) { case (r, c) => r.addCookie(c) }).evalTap { resp =>
          F.blocking(
            resp.headers.headers
              .filter(_.name === `Set-Cookie`.name)
              .flatMap(c => HttpCookie.parse(c.value).asScala)
              .foreach(hc => cookie_store.add(URI.create(req.uri.renderString), hc)))
        }
      } yield out
    }
  }
}
