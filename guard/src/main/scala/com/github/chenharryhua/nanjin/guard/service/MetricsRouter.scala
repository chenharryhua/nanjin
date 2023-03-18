package com.github.chenharryhua.nanjin.guard.service

import cats.Monad
import cats.data.Kleisli
import cats.implicits.toShow
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, Snapshot}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.server.Router
import org.http4s.{HttpRoutes, Request, Response}

private class MetricsRouter[F[_]: Monad](mr: MetricRegistry, sp: ServiceParams) extends Http4sDsl[F] {
  private def gauges(g: Snapshot.Gauge): Json         = Json.obj(g.id.show -> g.value)
  private def counters(c: Snapshot.Counter): Json     = Json.obj(c.id.show -> Json.fromLong(c.count))
  private def meters(m: Snapshot.Meter): Json         = Json.obj(m.id.show -> m.data.asJson)
  private def histograms(h: Snapshot.Histogram): Json = Json.obj(h.id.show -> h.data.asJson)
  private def timers(t: Snapshot.Timer): Json         = Json.obj(t.id.show -> t.data.asJson)

  private val metrics = HttpRoutes.of[F] {
    // all
    case GET -> Root =>
      val ms = MetricSnapshot(mr)
      val res = ms.gauges.map(gauges) :::
        ms.counters.map(counters) :::
        ms.meters.map(meters) :::
        ms.histograms.map(histograms) :::
        ms.timers.map(timers)
      Ok(res.asJson)
    // counters
    case GET -> Root / "counters" => Ok(MetricSnapshot.counters(mr).map(counters).asJson)
    case GET -> Root / "counter" / name =>
      MetricSnapshot.counters(mr).find(_.isMatch(name)) match {
        case Some(value) => Ok(counters(value))
        case None        => NotFound()
      }
    // gauges
    case GET -> Root / "gauges" => Ok(MetricSnapshot.gauges(mr).map(gauges).asJson)
    case GET -> Root / "gauge" / name =>
      MetricSnapshot.gauges(mr).find(_.isMatch(name)) match {
        case Some(value) => Ok(gauges(value))
        case None        => NotFound()
      }
    // timers
    case GET -> Root / "timers" => Ok(MetricSnapshot.timers(mr).map(timers).asJson)
    case GET -> Root / "timer" / name =>
      MetricSnapshot.timers(mr).find(_.isMatch(name)) match {
        case Some(value) => Ok(timers(value))
        case None        => NotFound()
      }
    // histograms
    case GET -> Root / "histograms" => Ok(MetricSnapshot.histograms(mr).map(histograms).asJson)
    case GET -> Root / "histogram" / name =>
      MetricSnapshot.histograms(mr).find(_.isMatch(name)) match {
        case Some(value) => Ok(histograms(value))
        case None        => NotFound()
      }
  }

  private val service = HttpRoutes.of[F] { case GET -> Root => Ok(sp.asJson) }

  val router: Kleisli[F, Request[F], Response[F]] =
    Router("metrics" -> metrics, "service" -> service).orNotFound
}
