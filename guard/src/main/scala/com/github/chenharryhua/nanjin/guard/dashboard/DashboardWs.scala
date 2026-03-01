package com.github.chenharryhua.nanjin.guard.dashboard

import cats.data.Kleisli
import cats.effect.kernel.Async
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.scalatags.*
import org.http4s.server.Router
import org.http4s.server.websocket.WebSocketBuilder2
import org.http4s.{HttpRoutes, Request, Response, StaticFile}
import scalatags.Text
import scalatags.Text.all.*

import java.time.ZoneId

final class DashboardWs[F[_]](
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  zoneId: ZoneId,
  policy: Policy)(implicit F: Async[F])
    extends Http4sDsl[F] {

  private val pump = new MetricsPump(metricRegistry)

  private val html_page: Text.TypedTag[String] =
    html(
      head(
        tag("title")(serviceParams.serviceName.value),
        script(src := "https://cdn.jsdelivr.net/npm/chart.js"),
        script(src := "https://cdn.jsdelivr.net/npm/luxon"),
        script(src := "https://cdn.jsdelivr.net/npm/chartjs-adapter-luxon")
      ),
      body(
        div(
          id              := "chart-root",
          data("ws_port") := serviceParams.host.port.map(_.value).getOrElse(1026),
          data("zone_id") := zoneId.toString
        ),
        script(`type` := "module", src := "/dashboard/frontend.js")
      )
    )

  private def metrics(wsb2: WebSocketBuilder2[F]): HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root / "dashboard" / file =>
      StaticFile.fromResource(s"dashboard/$file").getOrElseF(NotFound())

    case GET -> Root / "dashboard" => Ok(html_page)

    case GET -> Root / "ws" => pump.pumping(wsb2, zoneId, policy)

    case GET -> Root / "report" => Ok("good")
  }

  def dashboardRouter(wsb2: WebSocketBuilder2[F]): Kleisli[F, Request[F], Response[F]] =
    Router("/" -> metrics(wsb2)).orNotFound
}
