package com.github.chenharryhua.nanjin.guard.service

import cats.Applicative
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.{Error, Event, ServiceStopCause}
import com.github.chenharryhua.nanjin.guard.translator.{textConstants, textHelper, Translator}

private object SimpleTextTranslator {
  import Event.*
  import textConstants.*

  private def service_event(se: Event): String = {
    val host: String = show"$CONSTANT_HOST:${se.serviceParams.host}"
    val sn: String = s"$CONSTANT_SERVICE:${se.serviceParams.serviceName.value}"
    val tn: String = s"$CONSTANT_TASK:${se.serviceParams.taskName.value}"
    val serviceId: String = s"$CONSTANT_SERVICE_ID:${se.serviceParams.serviceId.show}"
    val uptime: String = s"$CONSTANT_UPTIME:${textHelper.uptimeText(se)}"
    s"""|$sn, $tn, $uptime
        |  $host
        |  $serviceId""".stripMargin

  }

  private def error_str(err: Error): String =
    s"""Cause:${err.stack.mkString("\n\t")}"""

  private def service_start(evt: ServiceStart): String = {
    val idx = s"$CONSTANT_INDEX:${evt.tick.index}"
    val snz = s"$CONSTANT_SNOOZED:${textHelper.tookText(evt.tick.snooze)}"
    s"""|
        |  ${service_event(evt)}
        |  $idx, $snz
        |${interpret_service_params(evt.serviceParams).spaces2}
        |""".stripMargin
  }

  private def service_panic(evt: ServicePanic): String = {
    val idx = s"$CONSTANT_INDEX:${evt.tick.index}"
    val act = s"$CONSTANT_ACTIVE:${textHelper.tookText(evt.tick.active)}"
    show"""|
           |  ${service_event(evt)}
           |  $CONSTANT_POLICY:${evt.serviceParams.servicePolicies.restart.policy}
           |  ${textHelper.panicText(evt)}
           |  $idx, $act
           |  ${error_str(evt.error)}
           |""".stripMargin
  }

  private def service_stop(evt: ServiceStop): String = {
    def stopCause(ssc: ServiceStopCause): String = ssc match {
      case ServiceStopCause.Successfully       => "Successfully"
      case ServiceStopCause.ByCancellation     => "ByCancellation"
      case ServiceStopCause.ByException(error) => error.stack.mkString("\n\t")
      case ServiceStopCause.Maintenance        => "Maintenance"
    }
    show"""|
           |  ${service_event(evt)}
           |  $CONSTANT_POLICY:${evt.serviceParams.servicePolicies.restart.policy}
           |  $CONSTANT_CAUSE:${stopCause(evt.cause)}
           |""".stripMargin
  }

  private def metric_report(evt: MetricReport): String = {
    val policy = s"$CONSTANT_POLICY:${evt.serviceParams.servicePolicies.metricReport.show}"
    val took = s"$CONSTANT_TOOK:${textHelper.tookText(evt.took)}"
    val index = s"$CONSTANT_INDEX:${textHelper.metricIndexText(evt.index)}"

    s"""|
        |  ${service_event(evt)}
        |  $index, $policy, $took
        |${textHelper.yamlMetrics(evt.snapshot)}
        |""".stripMargin
  }

  private def metric_reset(evt: MetricReset): String = {
    val policy = s"$CONSTANT_POLICY:${evt.serviceParams.servicePolicies.metricReset.show}"
    val took = s"$CONSTANT_TOOK:${textHelper.tookText(evt.took)}"
    val index = s"$CONSTANT_INDEX:${textHelper.metricIndexText(evt.index)}"

    s"""|
        |  ${service_event(evt)}
        |  $index, $policy, $took
        |${textHelper.yamlMetrics(evt.snapshot)}
        |""".stripMargin
  }

  private def service_message(evt: ServiceMessage): String = {
    val host: String = show"$CONSTANT_HOST:${evt.serviceParams.host}"
    val sn: String = s"$CONSTANT_SERVICE:${evt.serviceParams.serviceName.value}"
    val tn: String = s"$CONSTANT_TASK:${evt.serviceParams.taskName.value}"
    val serviceId: String = s"$CONSTANT_SERVICE_ID:${evt.serviceParams.serviceId.show}"
    val uptime: String = s"$CONSTANT_UPTIME:${textHelper.uptimeText(evt)}"
    val token = s"$CONSTANT_MESSAGE_TOKEN:${evt.token}"
    val domain = s"$CONSTANT_DOMAIN:${evt.domain.value}"

    s"""|
        |  $sn, $tn, $domain, $uptime
        |  $host
        |  $serviceId, $token
        |${evt.message.spaces2}
        |${evt.error.fold("")(error_str)}
        |""".stripMargin
  }

  def apply[F[_]: Applicative]: Translator[F, String] =
    Translator
      .empty[F, String]
      .withServiceStart(service_start)
      .withServiceStop(service_stop)
      .withServicePanic(service_panic)
      .withMetricReport(metric_report)
      .withMetricReset(metric_reset)
      .withServiceMessage(service_message)
}
