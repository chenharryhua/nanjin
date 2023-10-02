package com.github.chenharryhua.nanjin.guard.translators

import cats.Applicative
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.{NJError, NJEvent}
import io.circe.syntax.EncoderOps

private object SimpleTextTranslator {
  import NJEvent.*
  import textConstants.*
  import textHelper.*

  private def serviceEvent(se: NJEvent): String = {
    val host: String      = s"$CONSTANT_HOST:${se.serviceParams.taskParams.hostName.value}"
    val sn: String        = s"$CONSTANT_SERVICE:${se.serviceParams.serviceName}"
    val tn: String        = s"$CONSTANT_TASK:${se.serviceParams.taskParams.taskName}"
    val serviceId: String = s"SID:${se.serviceParams.serviceId.show.takeRight(12)}"
    val uptime: String    = s"$CONSTANT_UPTIME:${upTimeText(se)}"
    s"$sn, $tn, $host, $serviceId, $uptime"
  }

  private def errorStr(err: NJError): String = s"Cause:${err.stackTrace}"

  private def actionEvent(ae: ActionEvent): String = {
    val id  = s"$CONSTANT_ACTION_ID:${ae.actionId.show}"
    val mm  = s"$CONSTANT_MEASUREMENT:${ae.actionParams.metricName.measurement}"
    val cfg = s"$CONSTANT_CONFIG:${ae.actionParams.configStr}"

    s"""  ${serviceEvent(ae)}
       |  $id, $mm, $cfg""".stripMargin
  }

  private def serviceStarted(evt: ServiceStart): String =
    s"""${eventTitle(evt)}
       |  ${serviceEvent(evt)}
       |${evt.serviceParams.asJson.spaces2}
       |""".stripMargin

  private def servicePanic(evt: ServicePanic): String =
    s"""${eventTitle(evt)}
       |  ${serviceEvent(evt)}
       |  ${panicText(evt)}
       |  $CONSTANT_POLICY:${evt.serviceParams.servicePolicies.restart}
       |  ${errorStr(evt.error)}
       |""".stripMargin

  private def serviceStopped(evt: ServiceStop): String =
    s"""${eventTitle(evt)}
       |  ${serviceEvent(evt)}
       |  $CONSTANT_POLICY:${evt.serviceParams.servicePolicies.restart}
       |  $CONSTANT_CAUSE:${evt.cause.show}
       |""".stripMargin

  private def metricReport(evt: MetricReport): String =
    s"""${eventTitle(evt)}
       |  ${serviceEvent(evt)}
       |  $CONSTANT_POLICY:${evt.serviceParams.servicePolicies.metricReport}
       |${yamlMetrics(evt.snapshot, evt.serviceParams.metricParams)}
       |""".stripMargin

  private def metricReset(evt: MetricReset): String =
    s"""${eventTitle(evt)}
       |  ${serviceEvent(evt)}
       |  $CONSTANT_POLICY:${evt.serviceParams.servicePolicies.metricReset}
       |${yamlMetrics(evt.snapshot, evt.serviceParams.metricParams)}
       |""".stripMargin

  private def serviceAlert(evt: ServiceAlert): String =
    s"""${eventTitle(evt)}
       |  ${serviceEvent(evt)}
       |${evt.message.spaces2}
       |""".stripMargin

  private def actionStart(evt: ActionStart): String =
    s"""${eventTitle(evt)}
       |${actionEvent(evt)}
       |${evt.notes.fold("")(_.spaces2)}
       |""".stripMargin

  private def actionRetry(evt: ActionRetry): String =
    s"""${eventTitle(evt)}
       |${actionEvent(evt)}
       |  $CONSTANT_SNOOZE:${tookText(evt.tick.snooze)}
       |  $CONSTANT_POLICY:${evt.actionParams.retryPolicy}
       |  ${errorStr(evt.error)}
       |""".stripMargin

  private def actionFail(evt: ActionFail): String =
    s"""${eventTitle(evt)}
       |${actionEvent(evt)}
       |  $CONSTANT_TOOK:${tookText(evt.took)}
       |  $CONSTANT_POLICY:${evt.actionParams.retryPolicy}
       |  ${errorStr(evt.error)}
       |${evt.notes.fold("")(_.spaces2)}
       |""".stripMargin

  private def actionDone(evt: ActionDone): String =
    s"""${eventTitle(evt)}
       |${actionEvent(evt)}
       |  $CONSTANT_TOOK:${tookText(evt.took)}
       |${evt.notes.fold("")(_.spaces2)}
       |""".stripMargin

  def apply[F[_]: Applicative]: Translator[F, String] =
    Translator
      .empty[F, String]
      .withServiceStart(serviceStarted)
      .withServiceStop(serviceStopped)
      .withServicePanic(servicePanic)
      .withMetricReport(metricReport)
      .withMetricReset(metricReset)
      .withServiceAlert(serviceAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetry)
      .withActionFail(actionFail)
      .withActionDone(actionDone)

}
