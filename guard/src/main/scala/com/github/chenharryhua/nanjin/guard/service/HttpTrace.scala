package com.github.chenharryhua.nanjin.guard.service

import cats.data.{Kleisli, OptionT}
import cats.effect.implicits.monadCancelOps
import cats.effect.kernel.{MonadCancel, Outcome, Resource}
import cats.syntax.all.*
import natchez.*
import org.apache.commons.lang3.exception.ExceptionUtils
import org.http4s.client.Client
import org.http4s.{Header, Headers, HttpRoutes, Response}
import org.typelevel.ci.*

private object HttpTrace {
  // copy from https://github.com/tpolecat/natchez-http4s
  private val ExcludedHeaders: Set[CIString] = {
    import org.http4s.headers.*
    val payload = Set(
      `Content-Length`.name,
      ci"Content-Type",
      `Content-Range`.name,
      ci"Trailer",
      `Transfer-Encoding`.name
    )

    val security: Set[CIString] = Set(
      Authorization.name,
      Cookie.name,
      `Set-Cookie`.name
    )
    payload ++ security
  }

  /** Notes: AWS X-Ray requires that annotation keys be strings of at most 200 characters that only contain
    * alphanumeric characters, hyphens, and underscores
    * https://docs.aws.amazon.com/xray/latest/devguide/xray-api-segmentdocuments.html
    */

  def server[F[_]](routes: Span[F] => HttpRoutes[F], entryPoint: Resource[F, EntryPoint[F]])(implicit
    F: MonadCancel[F, Throwable]): HttpRoutes[F] =
    Kleisli { req =>
      val kernelHeaders = req.headers.headers.collect {
        case header if !ExcludedHeaders.contains(header.name) => header.name -> header.value
      }.toMap

      val kernel = Kernel(kernelHeaders)
      val spanR  = entryPoint.flatMap(_.continueOrElseRoot(req.uri.path.toString, kernel))

      val response: F[Option[Response[F]]] = spanR.use { span =>
        val addRequestFields: F[Unit] =
          span.put("http_method" -> req.method.name, "http_url" -> req.uri.renderString)

        def addResponseFields(res: Response[F]): F[Unit] =
          span.put("http_status_code" -> res.status.code.toString)

        def addErrorFields(e: Throwable): F[Unit] =
          span.put(
            Tags.error(true),
            "error_message" -> ExceptionUtils.getMessage(e),
            "error_stacktrace" -> ExceptionUtils.getStackTrace(e)
          )

        routes(span)(req).value.guaranteeCase {
          case Outcome.Errored(e) => addRequestFields *> addErrorFields(e)
          case Outcome.Canceled() =>
            addRequestFields *> span.put(("cancelled", TraceValue.BooleanValue(true)), Tags.error(true))
          case Outcome.Succeeded(fa) =>
            fa.flatMap {
              case Some(resp) => addRequestFields *> addResponseFields(resp)
              case None       => F.unit
            }
        }
      }
      OptionT(response)
    }

  def client[F[_]](span: Span[F], client: Client[F])(implicit ev: MonadCancel[F, Throwable]): Client[F] =
    Client { req =>
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
