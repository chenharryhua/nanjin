package com.github.chenharryhua.nanjin.guard.service

import cats.Monad
import cats.data.Kleisli
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.MetricSnapshot
import com.github.chenharryhua.nanjin.guard.translators.SnapshotPolyglot
import io.circe.syntax.EncoderOps
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.scalatags.*
import org.http4s.server.Router
import org.http4s.{HttpRoutes, Request, Response}
import scalatags.Text.all.*

private class MetricsRouter[F[_]: Monad](mr: MetricRegistry, sp: ServiceParams) extends Http4sDsl[F] {
  private val metrics = HttpRoutes.of[F] {
    case GET -> Root / "metrics" =>
      Ok(new SnapshotPolyglot(MetricSnapshot(mr), sp.metricParams).toPrettyJson)
    case GET -> Root / "metrics" / "vanilla" =>
      Ok(new SnapshotPolyglot(MetricSnapshot(mr), sp.metricParams).toVanillaJson)
    case GET -> Root / "metrics" / "yaml" =>
      val text = new SnapshotPolyglot(MetricSnapshot(mr), sp.metricParams).toYaml
      Ok(html(body(pre(text))))
    case GET -> Root / "service" => Ok(sp.asJson)
  }

  val router: Kleisli[F, Request[F], Response[F]] =
    Router("/" -> metrics).orNotFound
}
