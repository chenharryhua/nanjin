package com.github.chenharryhua.nanjin.guard.service

import cats.Applicative
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.guard.config.Attribute
import com.github.chenharryhua.nanjin.guard.event.Event.*
import com.github.chenharryhua.nanjin.guard.event.{Index, MetricSnapshot}
import com.github.chenharryhua.nanjin.guard.translator.*
import io.circe.Json
import io.circe.syntax.EncoderOps

private object PrettyJsonTranslator {

  private def pretty_metrics(ss: MetricSnapshot): (String, Json) =
    "metrics" -> new SnapshotPolyglot(ss).toPrettyJson

  // events handlers
  private def service_start(evt: ServiceStart): Json =
    Json.obj(
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry,
      Attribute(evt.upTime).snakeJsonEntry(_.show.asJson),
      Attribute(Index(evt.tick.index)).snakeJsonEntry,
      "snoozed" -> Json.fromString(durationFormatter.format(evt.tick.snooze)),
      "params" -> evt.serviceParams.simpleJson
    )

  private def service_panic(evt: ServicePanic): Json =
    Json.obj(
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry,
      Attribute(evt.upTime).snakeJsonEntry(_.show.asJson),
      Attribute(Index(evt.tick.index)).snakeJsonEntry,
      "active" -> Json.fromString(durationFormatter.format(evt.tick.active)),
      "snooze" -> Json.fromString(durationFormatter.format(evt.tick.snooze)),
      Attribute(evt.serviceParams.servicePolicies.restart.policy).snakeJsonEntry(_.show.asJson),
      Attribute(evt.error).snakeJsonEntry
    )

  private def service_stop(evt: ServiceStop): Json =
    Json.obj(
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry,
      Attribute(evt.upTime).snakeJsonEntry(_.show.asJson),
      Attribute(evt.serviceParams.servicePolicies.restart.policy).snakeJsonEntry(_.show.asJson),
      Attribute(evt.cause).snakeJsonEntry
    )

  private def metrics_report(evt: MetricsReport): Json =
    Json.obj(
      Attribute(evt.index).snakeJsonEntry(_.show.asJson),
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry,
      Attribute(evt.serviceParams.servicePolicies.metricsReport).snakeJsonEntry(_.show.asJson),
      Attribute(evt.upTime).snakeJsonEntry(_.show.asJson),
      Attribute(evt.took).snakeJsonEntry(_.show.asJson),
      pretty_metrics(evt.snapshot)
    )

  private def metrics_reset(evt: MetricsReset): Json =
    Json.obj(
      Attribute(evt.index).snakeJsonEntry(_.show.asJson),
      Attribute(evt.serviceParams.serviceName).snakeJsonEntry,
      Attribute(evt.serviceParams.serviceId).snakeJsonEntry,
      Attribute(evt.serviceParams.servicePolicies.metricsReset).snakeJsonEntry(_.show.asJson),
      Attribute(evt.upTime).snakeJsonEntry(_.show.asJson),
      Attribute(evt.took).snakeJsonEntry(_.show.asJson),
      pretty_metrics(evt.snapshot)
    )

  private def service_message(evt: ServiceMessage): Json =
    jsonHelper.json_service_message(evt)

  def apply[F[_]: Applicative]: Translator[F, Json] =
    Translator
      .empty[F, Json]
      .withServiceStart(service_start)
      .withServiceStop(service_stop)
      .withServicePanic(service_panic)
      .withMetricsReport(metrics_report)
      .withMetricsReset(metrics_reset)
      .withServiceMessage(service_message)
}
