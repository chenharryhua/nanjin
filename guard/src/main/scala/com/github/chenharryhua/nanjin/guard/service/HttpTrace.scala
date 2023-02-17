package com.github.chenharryhua.nanjin.guard.service

import cats.data.{Kleisli, OptionT}
import cats.effect.implicits.monadCancelOps
import cats.effect.kernel.{MonadCancel, Outcome, Resource}
import cats.implicits.{catsSyntaxApply, toFlatMapOps}
import natchez.http4s.AnsiFilterStream
import natchez.http4s.syntax.EntryPointOps
import natchez.{EntryPoint, Kernel, Tags, TraceValue}
import org.http4s.{HttpRoutes, Response}

import java.io.{ByteArrayOutputStream, PrintStream}

private object HttpTrace {

  // combine EntryPoint#liftT and NatchezMiddleware together
  // https://github.com/tpolecat/natchez-http4s

  def server[F[_]](routes: HttpRoutes[F], entryPoint: Resource[F, EntryPoint[F]])(implicit
    F: MonadCancel[F, Throwable]): HttpRoutes[F] =
    Kleisli { req =>
      val kernelHeaders = req.headers.headers.collect {
        case header if !EntryPointOps.ExcludedHeaders.contains(header.name) => header.name -> header.value
      }.toMap

      val kernel = Kernel(kernelHeaders)
      val spanR  = entryPoint.flatMap(_.continueOrElseRoot(req.uri.path.toString, kernel))

      val response: F[Option[Response[F]]] = spanR.use { span =>
        val addRequestFields: F[Unit] =
          span.put(
            Tags.http.method(req.method.name),
            Tags.http.url(req.uri.renderString)
          )

        def addResponseFields(res: Response[F]): F[Unit] =
          span.put(
            Tags.http.status_code(res.status.code.toString)
          )

        def addErrorFields(e: Throwable): F[Unit] =
          span.put(
            Tags.error(true),
            "error.message" -> e.getMessage(),
            "error.stacktrace" -> {
              val baos = new ByteArrayOutputStream
              val fs   = new AnsiFilterStream(baos)
              val ps   = new PrintStream(fs, true, "UTF-8")
              e.printStackTrace(ps)
              ps.close()
              fs.close()
              baos.close()
              new String(baos.toByteArray, "UTF-8")
            }
          )

        routes(req).value.guaranteeCase {
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
}
