package com.github.chenharryhua.nanjin.guard.service

import cats.Monad
import cats.data.Kleisli
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, Snapshot}
import com.github.chenharryhua.nanjin.guard.translators.SnapshotJson
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.server.Router
import org.http4s.{HttpRoutes, Request, Response}

private class MetricsRouter[F[_]: Monad](mr: MetricRegistry, sp: ServiceParams) extends Http4sDsl[F] {
  private def gauges(g: Snapshot.Gauge): Json         = Json.obj(g.metricId.display -> g.value)
  private def counters(c: Snapshot.Counter): Json     = Json.obj(c.metricId.display -> Json.fromLong(c.count))
  private def meters(m: Snapshot.Meter): Json         = Json.obj(m.metricId.display -> m.meter.asJson)
  private def histograms(h: Snapshot.Histogram): Json = Json.obj(h.metricId.display -> h.histogram.asJson)
  private def timers(t: Snapshot.Timer): Json         = Json.obj(t.metricId.display -> t.timer.asJson)

  private val metrics = HttpRoutes.of[F] {
    // all
    case GET -> Root                => Ok(new SnapshotJson(MetricSnapshot(mr)).toPrettyJson(sp.metricParams))
    case GET -> Root / "counters"   => Ok(MetricSnapshot.counters(mr).map(counters).asJson)
    case GET -> Root / "gauges"     => Ok(MetricSnapshot.gauges(mr).map(gauges).asJson)
    case GET -> Root / "meters"     => Ok(MetricSnapshot.meters(mr).map(meters).asJson)
    case GET -> Root / "timers"     => Ok(MetricSnapshot.timers(mr).map(timers).asJson)
    case GET -> Root / "histograms" => Ok(MetricSnapshot.histograms(mr).map(histograms).asJson)
  }

  private val service = HttpRoutes.of[F] { case GET -> Root => Ok(sp.asJson) }

  val router: Kleisli[F, Request[F], Response[F]] =
    Router("metrics" -> metrics, "service" -> service).orNotFound
}
