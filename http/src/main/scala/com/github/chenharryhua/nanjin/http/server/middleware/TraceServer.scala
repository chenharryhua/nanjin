package com.github.chenharryhua.nanjin.http.server.middleware

import cats.data.{Kleisli, OptionT}
import cats.effect.implicits.monadCancelOps
import cats.effect.kernel.{MonadCancel, Outcome, Resource}
import cats.syntax.all.*
import natchez.*
import org.apache.commons.lang3.exception.ExceptionUtils
import org.http4s.{HttpRoutes, Response}
import org.typelevel.ci.*

object TraceServer {

  // Headers excluded from trace context (payload and sensitive)
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
  def apply[F[_]](
    entryPoint: Resource[F, EntryPoint[F]]
  )(routes: Span[F] => HttpRoutes[F])(implicit F: MonadCancel[F, Throwable]): HttpRoutes[F] =
    Kleisli { req =>
      val kernelHeaders = req.headers.headers.collect {
        case header if !ExcludedHeaders.contains(header.name) => header.name -> header.value
      }.toMap

      val kernel = Kernel(kernelHeaders)
      val spanResource: Resource[F, Span[F]] =
        entryPoint.flatMap(_.continueOrElseRoot(req.uri.path.toString, kernel))

      val responseF: F[Option[Response[F]]] = spanResource.use { span =>
        val addRequestFields = span.put(
          "http_method" -> req.method.name,
          "http_url" -> req.uri.renderString
        )

        for {
          // Run routes and capture response
          result <- routes(span)(req).value.guaranteeCase {
            case Outcome.Errored(e) =>
              addRequestFields *> span.put(
                Tags.error(true),
                "error_message" -> ExceptionUtils.getMessage(e),
                "error_stacktrace" -> ExceptionUtils.getStackTrace(e)
              )
            case Outcome.Canceled() =>
              addRequestFields *> span.put(
                ("cancelled", TraceValue.BooleanValue(true)),
                Tags.error(true)
              )
            case Outcome.Succeeded(fa) =>
              fa.flatMap {
                case Some(resp) =>
                  addRequestFields *> span.put("http_status_code" -> resp.status.code.toString)
                case None => F.unit
              }
          }
        } yield result
      }

      OptionT(responseF)
    }

  def apply[F[_]](
    entryPoint: EntryPoint[F]
  )(routes: Span[F] => HttpRoutes[F])(implicit F: MonadCancel[F, Throwable]): HttpRoutes[F] =
    apply(Resource.pure[F, EntryPoint[F]](entryPoint))(routes)
}
