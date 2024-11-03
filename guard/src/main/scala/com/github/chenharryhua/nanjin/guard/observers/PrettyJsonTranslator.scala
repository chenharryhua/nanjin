package com.github.chenharryhua.nanjin.guard.observers

import cats.Applicative
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, NJEvent}
import com.github.chenharryhua.nanjin.guard.translator.*
import io.circe.Json

import java.time.Duration

object PrettyJsonTranslator {

  private def took(dur: Duration): (String, Json) =
    "took" -> Json.fromString(fmt.format(dur))

  private def uptime(evt: NJEvent): (String, Json) =
    "upTime" -> Json.fromString(fmt.format(evt.upTime))

  private def pretty_metrics(ss: MetricSnapshot): (String, Json) =
    "metrics" -> new SnapshotPolyglot(ss).toPrettyJson

  private def active(tick: Tick): (String, Json) =
    "active" -> Json.fromString(fmt.format(tick.active))

  // events handlers
  private def service_started(evt: ServiceStart): Json =
    Json.obj(
      EventName.ServiceStart.camel ->
        Json.obj(
          jsonHelper.serviceParams(evt.serviceParams),
          uptime(evt),
          jsonHelper.index(evt.tick),
          "snoozed" -> Json.fromString(fmt.format(evt.tick.snooze))))

  private def service_panic(evt: ServicePanic): Json =
    Json.obj(
      EventName.ServicePanic.camel ->
        Json.obj(
          jsonHelper.serviceName(evt.serviceParams),
          jsonHelper.serviceId(evt.serviceParams),
          uptime(evt),
          jsonHelper.index(evt.tick),
          active(evt.tick),
          "snooze" -> Json.fromString(fmt.format(evt.tick.snooze)),
          jsonHelper.policy(evt.serviceParams.servicePolicies.restart),
          jsonHelper.stack(evt.error)
        ))

  private def service_stopped(evt: ServiceStop): Json =
    Json.obj(
      EventName.ServiceStop.camel ->
        Json.obj(
          jsonHelper.serviceName(evt.serviceParams),
          jsonHelper.serviceId(evt.serviceParams),
          uptime(evt),
          jsonHelper.policy(evt.serviceParams.servicePolicies.restart),
          jsonHelper.exitCode(evt.cause),
          jsonHelper.exitCause(evt.cause)
        ))

  private def metric_report(evt: MetricReport): Json =
    Json.obj(
      EventName.MetricReport.camel ->
        Json.obj(
          jsonHelper.metricIndex(evt.index),
          jsonHelper.serviceName(evt.serviceParams),
          jsonHelper.serviceId(evt.serviceParams),
          jsonHelper.policy(evt.serviceParams.servicePolicies.metricReport),
          uptime(evt),
          took(evt.took),
          pretty_metrics(evt.snapshot)
        ))

  private def metric_reset(evt: MetricReset): Json =
    Json.obj(
      EventName.MetricReset.camel ->
        Json.obj(
          jsonHelper.metricIndex(evt.index),
          jsonHelper.serviceName(evt.serviceParams),
          jsonHelper.serviceId(evt.serviceParams),
          jsonHelper.policy(evt.serviceParams.servicePolicies.metricReset),
          uptime(evt),
          took(evt.took),
          pretty_metrics(evt.snapshot)
        ))

  private def service_message(evt: ServiceMessage): Json =
    Json.obj(EventName.ServiceMessage.camel -> jsonHelper.jsonServiceMessage(evt))

  def apply[F[_]: Applicative]: Translator[F, Json] =
    Translator
      .empty[F, Json]
      .withServiceStart(service_started)
      .withServiceStop(service_stopped)
      .withServicePanic(service_panic)
      .withMetricReport(metric_report)
      .withMetricReset(metric_reset)
      .withServiceMessage(service_message)
}
