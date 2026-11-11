package com.github.chenharryhua.nanjin.http.client.middleware
import cats.effect.kernel.{Resource, Sync}
import org.http4s.client.Client
import org.http4s.{Header, Headers, Request}
import org.typelevel.ci.CIString

import java.net.{CookieManager, URI}
import scala.jdk.CollectionConverters.*

/** `cookieBox` is an HTTP client middleware that provides automatic cookie management using a provided
  * `java.net.CookieManager`.
  *
  * Before each request it asks the manager which cookies apply to the request URI and attaches the `Cookie`
  * header it returns, merged with any cookie header the caller already set. After the response arrives it
  * hands the response headers back to the manager, which parses `Set-Cookie` and stores what its policy
  * accepts.
  *
  * ==Why the `CookieManager` and not its store==
  *
  * Both directions go through `CookieManager`, never through `getCookieStore`, because the manager is where
  * the cookie rules live. Reading through it applies RFC 2965 path matching, drops a `Secure` cookie on a
  * non-`https` request, honours a `$Port` list, and orders cookies by path specificity. Writing through it
  * consults the manager's `CookiePolicy` and defaults a cookie's domain and path from the request URI. The
  * bare `CookieStore` applies domain matching alone, so talking to it directly would send `Secure` cookies
  * over plain HTTP, ignore path scoping, and store a cookie for whatever domain a response happened to name.
  *
  * ==Which cookies a policy accepts==
  *
  * `new CookieManager()` uses `CookiePolicy.ACCEPT_ORIGINAL_SERVER`, which accepts a cookie only when its
  * domain matches the host that set it, so a response cannot plant a cookie for an unrelated domain. A
  * `Set-Cookie` without a `Domain` is attributed to the request host, and a host with no dot in it is treated
  * as `<host>.local`, so a single-label host such as `localhost` works as expected. Pass
  * `new CookieManager(null, CookiePolicy.ACCEPT_ALL)` to accept cookies for any domain a response names, or
  * `ACCEPT_NONE` to keep the store empty.
  *
  * ==Resource safety and threading==
  *
  *   - The middleware preserves the `Resource` lifecycle of the wrapped client; response finalizers are
  *     untouched. Cookies are stored when the response is allocated, before its body is consumed.
  *   - Store reads and writes are suspended in `Sync[F].delay` and run on the compute pool. That suits the
  *     default in-memory store, whose work is parsing plus synchronized map access. A `CookieManager` built
  *     over a store that does real I/O would block a compute thread; give such a store its own blocking
  *     wrapper instead.
  *   - The Java SE `CookieManager` and its default store are thread-safe, so concurrent fibers need no
  *     further synchronization, though heavy contention serializes on the store's internal lock.
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
  * @param cookieManager
  *   the Java `CookieManager` that holds the cookies and decides which to accept
  * @param client
  *   the HTTP client to wrap
  * @tparam F
  *   the effect type, needs `Sync[F]` to suspend the Java cookie calls
  * @return
  *   a new `Client[F]` that automatically manages cookies
  */
def cookieBox[F[_]](cookieManager: CookieManager)(client: Client[F])(using F: Sync[F]): Client[F] =
  Client[F] { req =>
    for {
      uri <- Resource.eval(F.delay(URI.create(req.uri.renderString)))
      applicable <- Resource.eval(F.delay(cookieManager.get(uri, headers_as_java(req.headers))))
      out <- client.run(attach_cookies(req, applicable)).evalTap { resp =>
        F.delay(cookieManager.put(uri, headers_as_java(resp.headers)))
      }
    } yield out
  }

/** Headers in the shape the `java.net.CookieHandler` contract expects: field name to field values, with
  * repeated fields (several `Set-Cookie` headers) kept in order under one key.
  */
private def headers_as_java(headers: Headers): java.util.Map[String, java.util.List[String]] =
  headers.headers.groupMap(_.name.toString)(_.value).view.mapValues(_.asJava).toMap.asJava

/** Attach the cookie headers the manager produced, one value per applicable cookie, merging them with a
  * header of the same name the caller already set rather than replacing it.
  */
private def attach_cookies[F[_]](
  req: Request[F],
  cookies: java.util.Map[String, java.util.List[String]]): Request[F] =
  cookies.asScala.foldLeft(req) { case (request, (name, values)) =>
    values.asScala.toList match {
      case Nil => request
      case vs  =>
        val field: CIString = CIString(name)
        val existing: List[String] = request.headers.get(field).toList.flatMap(_.toList.map(_.value))
        request.putHeaders(Header.Raw(field, (existing ++ vs).mkString("; ")))
    }
  }
