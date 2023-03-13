package com.github.chenharryhua.nanjin.guard.service

import cats.Monad
import cats.data.Kleisli
import cats.implicits.catsSyntaxEq
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.MetricSnapshot
import io.circe.syntax.EncoderOps
import org.http4s.{HttpRoutes, Request, Response}
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.server.Router

private class MetricsRouter[F[_]: Monad](mr: MetricRegistry, sp: ServiceParams) extends Http4sDsl[F] {
  private val metrics = HttpRoutes.of[F] {
    // all
    case GET -> Root => Ok(MetricSnapshot(mr).asJson)
    // counters
    case GET -> Root / "counters" => Ok(MetricSnapshot.counters(mr).asJson)
    case GET -> Root / "counter" / name =>
      MetricSnapshot.counters(mr).find(id => id.digested.name === name || id.digested.digest === name) match {
        case Some(value) => Ok(value)
        case None        => NotFound()
      }
    // gauges
    case GET -> Root / "gauges" => Ok(MetricSnapshot.gauges(mr).asJson)
    case GET -> Root / "gauge" / name =>
      MetricSnapshot.gauges(mr).find(id => id.digested.name === name || id.digested.digest === name) match {
        case Some(value) => Ok(value)
        case None        => NotFound()
      }
    // timers
    case GET -> Root / "timers" => Ok(MetricSnapshot.timers(mr).asJson)
    case GET -> Root / "timer" / name =>
      MetricSnapshot.timers(mr).find(id => id.digested.name === name || id.digested.digest === name) match {
        case Some(value) => Ok(value)
        case None        => NotFound()
      }
    // histograms
    case GET -> Root / "histograms" => Ok(MetricSnapshot.histograms(mr).asJson)
    case GET -> Root / "histogram" / name =>
      MetricSnapshot
        .histograms(mr)
        .find(id => id.digested.name === name || id.digested.digest === name) match {
        case Some(value) => Ok(value)
        case None        => NotFound()
      }
  }

  private val service = HttpRoutes.of[F] { case GET -> Root => Ok(sp.asJson) }

  val router: Kleisli[F, Request[F], Response[F]] =
    Router("metrics" -> metrics, "service" -> service).orNotFound
}
