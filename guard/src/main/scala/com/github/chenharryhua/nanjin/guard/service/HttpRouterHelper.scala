package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Clock, Sync}
import cats.effect.std.AtomicCell
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.either.catsSyntaxEitherId
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.show.toShow
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.{MetricsReport, ServiceMessage, ServicePanic}
import com.github.chenharryhua.nanjin.guard.event.{
  retrieveHealthChecks,
  Active,
  Index,
  ScrapeMode,
  Snapshot,
  Snooze,
  Timestamp,
  Took
}
import com.github.chenharryhua.nanjin.guard.translator.htmlHelper.htmlColoring
import com.github.chenharryhua.nanjin.guard.translator.{
  durationFormatter,
  prettifyJson,
  Attribute,
  SnapshotPolyglot
}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}
import scala.jdk.CollectionConverters.IteratorHasAsScala

final private class HttpRouterHelper[F[_]: Sync](
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  panicHistory: AtomicCell[F, CircularFifoQueue[ServicePanic]],
  metricsHistory: AtomicCell[F, CircularFifoQueue[MetricsReport]],
  errorHistory: AtomicCell[F, CircularFifoQueue[ServiceMessage]])
    extends all {

  private case class Present(value: ZonedDateTime) {
    val text: String = value.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show
    val json: Json = text.asJson
  }
  private case class Age(value: Duration) {
    val json: Json = durationFormatter.format(value).asJson
  }

  val html_header: Text.TypedTag[String] =
    head(
      tag("style")("""
        td, th {text-align: left; padding: 2px; border: 1px solid;}
        table {
          border-collapse: collapse;
          width: 70%;
        }
      """),
      tag("title")(serviceParams.serviceName.value)
    )

  def html_table_title(now: ZonedDateTime, took: Duration): Text.TypedTag[String] = {
    val service_name = Attribute(serviceParams.serviceName).textEntry
    val policy = Attribute(serviceParams.servicePolicies.metricsReport).textEntry
    val timezone = Attribute(serviceParams.timeZone).textEntry
    val uptime = Attribute(serviceParams.upTime(now)).textEntry
    val spend = Attribute(Took(took)).textEntry
    val present = Attribute(Present(now)).textEntry(_.text)

    table(
      tr(
        th(service_name.tag),
        th(policy.tag),
        th(timezone.tag),
        th(uptime.tag),
        th(spend.tag),
        th(present.tag)),
      tr(
        td(service_name.text),
        td(policy.text),
        td(timezone.text),
        td(uptime.text),
        td(spend.text),
        td(present.text))
    )
  }

  val metrics_yaml: F[Text.TypedTag[String]] =
    serviceParams.zonedNow.flatMap { now =>
      Snapshot.timed[F](metricRegistry, ScrapeMode.Full).map { case (fd, ms) =>
        val yaml = new SnapshotPolyglot(ms).toYaml
        html(html_header, body(div(html_table_title(now, fd), pre(yaml))))
      }
    }

  val metrics_vanilla: F[Json] =
    Snapshot.timed[F](metricRegistry, ScrapeMode.Full).map { case (fd, ms) =>
      Json.obj(
        Attribute(serviceParams.serviceName).snakeJsonEntry,
        Attribute(Took(fd)).map(_.show).snakeJsonEntry,
        Attribute(ms).map(new SnapshotPolyglot(_).toVanillaJson).snakeJsonEntry
      )
    }

  val metrics_json: F[Json] =
    Snapshot.timed[F](metricRegistry, ScrapeMode.Full).map { case (fd, ms) =>
      Json.obj(
        Attribute(serviceParams.serviceName).snakeJsonEntry,
        Attribute(Took(fd)).map(_.show).snakeJsonEntry,
        Attribute(ms).map(new SnapshotPolyglot(_).toPrettyJson).snakeJsonEntry
      )
    }

  val metrics_raw_json: F[Json] =
    Snapshot.timed[F](metricRegistry, ScrapeMode.Full).map { case (fd, ms) =>
      Json.obj(
        Attribute(serviceParams.serviceName).snakeJsonEntry,
        Attribute(Took(fd)).map(_.show).snakeJsonEntry,
        Attribute(ms).map(_.sorted.asJson).snakeJsonEntry
      )
    }

  val metrics_history: F[Text.TypedTag[String]] = {
    val text: F[Text.TypedTag[String]] =
      serviceParams.zonedNow.flatMap { now =>
        val history: F[List[Text.TypedTag[String]]] =
          metricsHistory.get.map(_.iterator().asScala.toList.reverse.flatMap { mr =>
            val took = Attribute(mr.took).textEntry
            val (index_tag, index) = Attribute(mr.index).entry {
              case Index.Adhoc(_)       => None
              case Index.Periodic(tick) =>
                val timestamp = Attribute(Timestamp(tick.zoned(_.conclude))).map(_.show).textEntry
                Some((tick.index, timestamp))
            }
            index.map { case (idx, timestamp) =>
              div(
                table(
                  tr(th(style := htmlColoring(mr))(index_tag), th(timestamp.tag), th(took.tag)),
                  tr(td(idx), td(timestamp.text), td(took.text))
                ),
                pre(new SnapshotPolyglot(mr.snapshot).toYaml)
              )
            }
          })
        history.map(hist => div(html_table_title(now, Duration.ZERO), h3("Metrics Report History"), hist))
      }

    text.map(t => html(html_header, body(div(t))))
  }

  def jvm_state: Json = prettifyJson(mxBeans.allJvmGauge.value.asJson)

  val service_panic_history: F[Json] =
    serviceParams.zonedNow.flatMap { now =>
      panicHistory.get.map(_.iterator().asScala.toList).map { panics =>
        val isActive = panics.lastOption.map(_.tick.conclude).forall(_.isBefore(now.toInstant))

        Json.obj(
          Attribute(serviceParams.serviceName).snakeJsonEntry,
          Attribute(serviceParams.serviceId).snakeJsonEntry,
          "is_active" -> Json.fromBoolean(isActive),
          Attribute(Present(now)).snakeJsonEntry(_.json),
          Attribute(serviceParams.servicePolicies.restart.policy).map(_.show).snakeJsonEntry,
          Attribute(serviceParams.timeZone).snakeJsonEntry,
          Attribute(serviceParams.upTime(now)).snakeJsonEntry(_.show.asJson),
          "panics" -> panics.size.asJson,
          "history" ->
            panics.reverse.map { sp =>
              Json.obj(
                "index" -> Json.fromLong(sp.tick.index),
                Attribute(Age(Duration.between(sp.timestamp.value, now))).map(_.json).snakeJsonEntry,
                "up_rouse_at" -> sp.tick.local(_.commence).asJson,
                Attribute(Active(sp.tick.active)).map(_.show.asJson).snakeJsonEntry,
                Attribute(Timestamp(sp.tick.zoned(_.acquires)))
                  .map(_.value.toLocalDateTime.asJson)
                  .snakeJsonEntry,
                Attribute(Snooze(sp.tick.snooze)).map(_.show).snakeJsonEntry,
                "restart_at" -> sp.tick.local(_.conclude).asJson,
                Attribute(sp.stackTrace).snakeJsonEntry
              )
            }.asJson
        )
      }
    }

  val service_error_history: F[Json] =
    serviceParams.zonedNow.flatMap { now =>
      errorHistory.get.map(_.iterator().asScala.toList).map { serviceMessages =>
        Json.obj(
          Attribute(serviceParams.serviceName).snakeJsonEntry,
          Attribute(serviceParams.serviceId).snakeJsonEntry,
          Attribute(Present(now)).map(_.json).snakeJsonEntry,
          Attribute(serviceParams.timeZone).snakeJsonEntry,
          Attribute(serviceParams.upTime(now)).map(_.show).snakeJsonEntry,
          "errors" -> serviceMessages.size.asJson,
          "history" -> serviceMessages.reverse.map { sm =>
            Json.obj(
              Attribute(sm.domain).snakeJsonEntry,
              Attribute(sm.correlation).snakeJsonEntry,
              Attribute(Age(Duration.between(sm.timestamp.value, now))).map(_.json).snakeJsonEntry,
              Attribute(sm.timestamp).map(_.value.toLocalDateTime.asJson).snakeJsonEntry,
              Attribute(sm.message).snakeJsonEntry,
              Attribute(sm.stackTrace).snakeJsonEntry
            )
          }.asJson
        )
      }
    }

  val service_health_check: F[Either[String, Json]] = {
    val deps_health_check: F[Json] =
      serviceParams.zonedNow[F].flatMap { now =>
        Snapshot.timed[F](metricRegistry, ScrapeMode.Full).map { case (fd, ss) =>
          Json.obj(
            "healthy" -> retrieveHealthChecks(ss.gauges).values.forall(identity).asJson,
            Attribute(Took(fd)).snakeJsonEntry(_.show.asJson),
            Attribute(Timestamp(now)).snakeJsonEntry(_.show.asJson)
          )
        }
      }

    panicHistory.get.map(_.iterator().asScala.toList.lastOption).flatMap {
      case None      => deps_health_check.map(Right(_))
      case Some(evt) =>
        Clock[F].realTimeInstant.flatMap { now =>
          if (evt.tick.conclude.isAfter(now)) {
            val recover = Duration.between(now, evt.tick.conclude)
            s"Service panic! Restart will be in ${durationFormatter.format(recover)}".asLeft[Json].pure[F]
          } else {
            deps_health_check.map(Right(_))
          }
        }
    }
  }
}
