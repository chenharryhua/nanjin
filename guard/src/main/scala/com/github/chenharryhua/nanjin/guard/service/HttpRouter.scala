package com.github.chenharryhua.nanjin.guard.service

import cats.data.Kleisli
import cats.effect.kernel.{Async, Clock}
import cats.effect.std.AtomicCell
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{MetricReport, ServicePanic}
import com.github.chenharryhua.nanjin.guard.event.{
  retrieveHealthChecks,
  MetricIndex,
  MetricSnapshot,
  NJEvent,
  ServiceStopCause
}
import com.github.chenharryhua.nanjin.guard.translator.{fmt, SnapshotPolyglot}
import fs2.concurrent.Channel
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.http4s.scalatags.*
import org.http4s.server.Router
import org.http4s.{HttpRoutes, Request, Response}
import scalatags.Text.all.*

import java.time.Duration
import scala.jdk.CollectionConverters.IteratorHasAsScala

private class HttpRouter[F[_]](
  metricRegistry: MetricRegistry,
  serviceParams: ServiceParams,
  panicHistory: AtomicCell[F, CircularFifoQueue[ServicePanic]],
  metricsHistory: AtomicCell[F, CircularFifoQueue[MetricReport]],
  channel: Channel[F, NJEvent])(implicit F: Async[F])
    extends Http4sDsl[F] {

  private val dependenciesHealthCheck: F[Json] =
    serviceParams.zonedNow[F].flatMap { now =>
      F.timed(F.delay(retrieveHealthChecks(metricRegistry).values.forall(identity))).map { case (fd, b) =>
        Json.obj("healthy" -> b.asJson, "took" -> fmt.format(fd).asJson, "when" -> now.toLocalTime.asJson)
      }
    }

  private val metrics = HttpRoutes.of[F] {
    case GET -> Root / "metrics" =>
      Ok(new SnapshotPolyglot(MetricSnapshot(metricRegistry)).toPrettyJson)

    case GET -> Root / "metrics" / "vanilla" =>
      Ok(new SnapshotPolyglot(MetricSnapshot(metricRegistry)).toVanillaJson)

    case GET -> Root / "metrics" / "yaml" =>
      val text = new SnapshotPolyglot(MetricSnapshot(metricRegistry)).toYaml
      Ok(html(body(pre(text))))

    case GET -> Root / "metrics" / "reset" =>
      for {
        ts <- serviceParams.zonedNow
        _ <- publisher.metricReset[F](channel, serviceParams, metricRegistry, MetricIndex.Adhoc(ts))
        res <- Ok(new SnapshotPolyglot(MetricSnapshot(metricRegistry)).toPrettyJson)
      } yield res

    case GET -> Root / "metrics" / "jvm" =>
      Ok(mxBeans.allJvmGauge.value.asJson)

    case GET -> Root / "metrics" / "history" =>
      val json = serviceParams.zonedNow.flatMap { now =>
        val history: F[List[Json]] =
          metricsHistory.get.map(_.iterator().asScala.toList.reverse.flatMap { mr =>
            mr.index match {
              case _: MetricIndex.Adhoc => None
              case MetricIndex.Periodic(tick) =>
                Some(
                  Json.obj(
                    "index" -> tick.index.asJson,
                    "launch_time" -> tick.zonedWakeup.toLocalDateTime.asJson,
                    "took" -> fmt.format(mr.took).asJson,
                    "metrics" -> new SnapshotPolyglot(mr.snapshot).toPrettyJson
                  ))
            }
          })
        history.map { hist =>
          Json.obj(
            "report_policy" -> serviceParams.servicePolicies.metricReport.show.asJson,
            "time_zone" -> serviceParams.zoneId.asJson,
            "up_time" -> fmt.format(serviceParams.upTime(now)).asJson,
            "history" -> hist.asJson
          )
        }
      }
      Ok(json)

    case GET -> Root / "service" => Ok(serviceParams.asJson)

    case GET -> Root / "service" / "stop" =>
      Ok("stopping service") <* publisher.serviceStop[F](channel, serviceParams, ServiceStopCause.Maintenance)

    case GET -> Root / "service" / "health_check" =>
      panicHistory.get.map(_.iterator().asScala.toList.lastOption).flatMap {
        case None => dependenciesHealthCheck.flatMap(Ok(_))
        case Some(evt) =>
          Clock[F].realTimeInstant.flatMap { now =>
            if (evt.tick.wakeup.isAfter(now)) {
              val recover = Duration.between(now, evt.tick.wakeup)
              ServiceUnavailable(s"Service panic! Restart will be in ${fmt.format(recover)}")
            } else {
              dependenciesHealthCheck.flatMap(Ok(_))
            }
          }
      }

    case GET -> Root / "service" / "history" =>
      serviceParams.zonedNow.flatMap { now =>
        panicHistory.get.map(_.iterator().asScala.toList).flatMap { panics =>
          val json: Json =
            Json.obj(
              "restart_policy" -> serviceParams.servicePolicies.restart.show.asJson,
              "zone_id" -> serviceParams.zoneId.asJson,
              "up_time" -> fmt.format(serviceParams.upTime(now)).asJson,
              "history" ->
                panics.reverse.map { sp =>
                  Json.obj(
                    "index" -> sp.tick.index.asJson,
                    "took_place" -> sp.tick.zonedAcquire.toLocalDateTime.asJson,
                    "restart_at" -> sp.tick.zonedWakeup.toLocalDateTime.asJson,
                    "snooze" -> fmt.format(sp.tick.snooze).asJson,
                    "caused_by" -> sp.error.message.asJson
                  )
                }.asJson
            )
          Ok(json)
        }
      }
  }

  val router: Kleisli[F, Request[F], Response[F]] =
    Router("/" -> metrics).orNotFound
}
