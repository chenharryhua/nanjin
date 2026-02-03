package com.github.chenharryhua.nanjin.http.client.middleware

import cats.effect.kernel.*
import cats.syntax.all.*
import natchez.Span
import org.http4s.{Header, Headers, Response}
import org.http4s.client.Client

object traceClient {
  // steal from https://github.com/typelevel/natchez-http4s/blob/main/modules/http4s/src/main/scala/natchez/http4s/NatchezMiddleware.scala
  // replace . with _
  def apply[F[_]](parent: Span[F])(client: Client[F])(implicit ev: MonadCancel[F, Throwable]): Client[F] =
    Client { req =>
      parent.span("http4s-client-request").flatMap { span =>
        val cc: F[(Response[F], F[Unit])] = for {
          knl <- span.kernel
          _ <- span.put(
            "client_http_uri" -> req.uri.toString(),
            "client_http_method" -> req.method.toString
          )
          hs = Headers(knl.toHeaders.map { case (k, v) => Header.Raw(k, v) }.toList)
          nReq = req.withHeaders(hs ++ req.headers)
          rsrc <- client.run(nReq).allocated
          _ <- span.put("client_http_status_code" -> rsrc._1.status.code.toString())
        } yield rsrc
        Resource(cc)
      }
    }
}
