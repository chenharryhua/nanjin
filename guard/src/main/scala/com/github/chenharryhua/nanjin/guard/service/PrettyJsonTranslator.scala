package com.github.chenharryhua.nanjin.guard.service

import cats.Applicative
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.event.Event.*
import com.github.chenharryhua.nanjin.guard.event.{Event, MetricSnapshot}
import com.github.chenharryhua.nanjin.guard.translator.*
import io.circe.Json

import java.time.Duration

private object PrettyJsonTranslator {

  private def took(dur: Duration): (String, Json) =
    "took" -> Json.fromString(durationFormatter.format(dur))

  private def uptime(evt: Event): (String, Json) =
    "up_time" -> Json.fromString(durationFormatter.format(evt.upTime))

  private def pretty_metrics(ss: MetricSnapshot): (String, Json) =
    "metrics" -> new SnapshotPolyglot(ss).toPrettyJson

  private def active(tick: Tick): (String, Json) =
    "active" -> Json.fromString(durationFormatter.format(tick.active))

  // events handlers
  private def service_start(evt: ServiceStart): Json =
    Json.obj(
      jsonHelper.service_name(evt.serviceParams),
      jsonHelper.service_id(evt.serviceParams),
      uptime(evt),
      jsonHelper.index(evt.tick),
      "snoozed" -> Json.fromString(durationFormatter.format(evt.tick.snooze)),
      "params" -> interpret_service_params(evt.serviceParams)
    )

  private def service_panic(evt: ServicePanic): Json =
    Json.obj(
      jsonHelper.service_name(evt.serviceParams),
      jsonHelper.service_id(evt.serviceParams),
      uptime(evt),
      jsonHelper.index(evt.tick),
      active(evt.tick),
      "snooze" -> Json.fromString(durationFormatter.format(evt.tick.snooze)),
      jsonHelper.policy(evt.serviceParams.servicePolicies.restart.policy),
      jsonHelper.stack(evt.error)
    )

  private def service_stop(evt: ServiceStop): Json =
    Json.obj(
      jsonHelper.service_name(evt.serviceParams),
      jsonHelper.service_id(evt.serviceParams),
      uptime(evt),
      jsonHelper.policy(evt.serviceParams.servicePolicies.restart.policy),
      jsonHelper.exit_code(evt.cause),
      jsonHelper.exit_cause(evt.cause)
    )

  private def metrics_report(evt: MetricsReport): Json =
    Json.obj(
      jsonHelper.metric_index(evt.index),
      jsonHelper.service_name(evt.serviceParams),
      jsonHelper.service_id(evt.serviceParams),
      jsonHelper.policy(evt.serviceParams.servicePolicies.metricsReport),
      uptime(evt),
      took(evt.took),
      pretty_metrics(evt.snapshot)
    )

  private def metrics_reset(evt: MetricsReset): Json =
    Json.obj(
      jsonHelper.metric_index(evt.index),
      jsonHelper.service_name(evt.serviceParams),
      jsonHelper.service_id(evt.serviceParams),
      jsonHelper.policy(evt.serviceParams.servicePolicies.metricsReset),
      uptime(evt),
      took(evt.took),
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
