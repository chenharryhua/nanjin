package com.github.chenharryhua.nanjin.guard.service

import cats.Monad
import cats.data.Kleisli
import cats.effect.kernel.Clock
import cats.implicits.catsSyntaxApplyOps
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, NJEvent, ServiceStopCause}
import com.github.chenharryhua.nanjin.guard.translators.SnapshotPolyglot
import fs2.concurrent.Channel
import io.circe.syntax.EncoderOps
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.scalatags.*
import org.http4s.server.Router
import org.http4s.{HttpRoutes, Request, Response}
import scalatags.Text.all.*

private class HttpRouter[F[_]: Monad: Clock](
  metricRegistry: MetricRegistry,
  serviceParams: ServiceParams,
  channel: Channel[F, NJEvent])
    extends Http4sDsl[F] {
  private val metrics = HttpRoutes.of[F] {
    case GET -> Root / "metrics" =>
      Ok(new SnapshotPolyglot(MetricSnapshot(metricRegistry)).toPrettyJson)
    case GET -> Root / "metrics" / "vanilla" =>
      Ok(new SnapshotPolyglot(MetricSnapshot(metricRegistry)).toVanillaJson)
    case GET -> Root / "metrics" / "yaml" =>
      val text = new SnapshotPolyglot(MetricSnapshot(metricRegistry)).toYaml
      Ok(html(body(pre(text))))
    case GET -> Root / "service" => Ok(serviceParams.asJson)
    case GET -> Root / "service" / "stop" =>
      Ok("stopping service") <* publisher.serviceStop[F](channel, serviceParams, ServiceStopCause.ByUser)
  }

  val router: Kleisli[F, Request[F], Response[F]] =
    Router("/" -> metrics).orNotFound
}
