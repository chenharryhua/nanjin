package com.github.chenharryhua.nanjin.guard.observers

import cats.Applicative
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.{NJError, NJEvent}
import com.github.chenharryhua.nanjin.guard.translator.metricConstants.METRICS_DIGEST
import com.github.chenharryhua.nanjin.guard.translator.{Translator, textConstants, textHelper}
import io.circe.syntax.EncoderOps

object SimpleTextTranslator {
  import NJEvent.*
  import textConstants.*
  import textHelper.*

  private def service_event(se: NJEvent): String = {
    val host: String      = s"$CONSTANT_HOST:${hostText(se.serviceParams)}"
    val sn: String        = s"$CONSTANT_SERVICE:${se.serviceParams.serviceName.value}"
    val tn: String        = s"$CONSTANT_TASK:${se.serviceParams.taskName.value}"
    val serviceId: String = s"$CONSTANT_SERVICE_ID:${se.serviceParams.serviceId.show}"
    val uptime: String    = s"$CONSTANT_UPTIME:${uptimeText(se)}"
    s"""|$sn, $tn, $serviceId, 
        |  $host, $uptime""".stripMargin
  }

  private def error_str(err: NJError): String =
    s"""Cause:${err.stack.mkString("\n\t")}"""

  private def service_started(evt: ServiceStart): String = {
    val idx = s"$CONSTANT_INDEX:${evt.tick.index}"
    val snz = s"$CONSTANT_SNOOZED:${tookText(evt.tick.snooze)}"
    s"""|${eventTitle(evt)}
        |  ${service_event(evt)}
        |  $idx, $snz
        |${evt.serviceParams.asJson.spaces2}
        |""".stripMargin
  }

  private def service_panic(evt: ServicePanic): String = {
    val idx = s"$CONSTANT_INDEX:${evt.tick.index}"
    val act = s"$CONSTANT_ACTIVE:${tookText(evt.tick.active)}"
    show"""|${eventTitle(evt)}
           |  ${service_event(evt)}
           |  $CONSTANT_POLICY:${evt.serviceParams.servicePolicies.restart}
           |  ${panicText(evt)}
           |  $idx, $act
           |  ${error_str(evt.error)}
           |""".stripMargin
  }

  private def service_stopped(evt: ServiceStop): String =
    show"""|${eventTitle(evt)}
           |  ${service_event(evt)}
           |  $CONSTANT_POLICY:${evt.serviceParams.servicePolicies.restart}
           |  $CONSTANT_CAUSE:${stopCause(evt.cause)}
           |""".stripMargin

  private def metric_report(evt: MetricReport): String = {
    val policy = evt.serviceParams.servicePolicies.metricReport.show
    val took   = tookText(evt.took)
    s"""|${eventTitle(evt)}
        |  ${service_event(evt)}
        |  $CONSTANT_INDEX:${metricIndexText(evt.index)}, $CONSTANT_POLICY:$policy, $CONSTANT_TOOK:$took
        |${yamlMetrics(evt.snapshot)}
        |""".stripMargin
  }

  private def metric_reset(evt: MetricReset): String = {
    val policy = evt.serviceParams.servicePolicies.metricReport.show
    val took   = tookText(evt.took)

    s"""|${eventTitle(evt)}
        |  ${service_event(evt)}
        |  $CONSTANT_INDEX:${metricIndexText(evt.index)}, $CONSTANT_POLICY:$policy, $CONSTANT_TOOK:$took
        |${yamlMetrics(evt.snapshot)}
        |""".stripMargin
  }

  private def service_message(evt: ServiceMessage): String = {
    val ms   = s"$CONSTANT_MEASUREMENT:${evt.metricName.measurement}"
    val id   = s"$METRICS_DIGEST:${evt.metricName.digest}"
    val name = s"$CONSTANT_NAME:${evt.metricName.name}"
    s"""|${eventTitle(evt)}
        |  ${service_event(evt)}
        |  $ms, $id, $name
        |${evt.message.spaces2}
        |""".stripMargin
  }

  def apply[F[_]: Applicative]: Translator[F, String] =
    Translator
      .empty[F, String]
      .withServiceStart(service_started)
      .withServiceStop(service_stopped)
      .withServicePanic(service_panic)
      .withMetricReport(metric_report)
      .withMetricReset(metric_reset)
      .withServiceMessage(service_message)
}
