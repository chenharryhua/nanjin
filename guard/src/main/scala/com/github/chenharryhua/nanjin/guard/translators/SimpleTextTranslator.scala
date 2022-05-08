package com.github.chenharryhua.nanjin.guard.translators

import cats.Applicative
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.*

private[translators] object SimpleTextTranslator {

  private def serviceEvent(se: ServiceEvent): String = {
    val host: String = se.serviceParams.taskParams.hostName.value
    val sn: String   = se.serviceParams.serviceName.value
    val up: String   = if (se.serviceStatus.isUp) s"Uptime:${fmt.format(se.upTime)}" else "Service is down"
    s"  Host:$host, ServiceID:${se.serviceID.show}, ServiceName:$sn, $up"
  }

  private def instantEvent(ie: InstantEvent): String = {
    val host: String = ie.serviceParams.taskParams.hostName.value
    s"""|  Host:$host, ServiceID:${ie.serviceID.show}
        |  Name:${ie.metricName.metricRepr}""".stripMargin
  }

  private def actionEvent(ae: ActionEvent): String = {
    val host: String = ae.serviceParams.taskParams.hostName.value
    s"""  Host:$host, ServiceID:${ae.serviceID.show}
       |  Name:${ae.metricName.metricRepr}, ID:${ae.actionID}""".stripMargin
  }

  private def serviceStarted(evt: ServiceStart): String =
    s"""Service (Re)Started
       |${serviceEvent(evt)}
       |""".stripMargin

  private def servicePanic(evt: ServicePanic): String =
    s"""Service Panic
       |${serviceEvent(evt)}
       |  Restarts:${evt.retryDetails.retriesSoFar}, ErrorID:${evt.error.uuid.show}
       |  StackTrace:${evt.error.stackTrace}
       |""".stripMargin

  private def serviceStopped(evt: ServiceStop): String =
    s"""Service Stopped
       |${serviceEvent(evt)}
       |  StackTrace:${evt.cause.show}
       |""".stripMargin

  private def metricReport(evt: MetricReport): String =
    s"""${evt.reportType.show}
       |${serviceEvent(evt)}
       |  OnGoings:${evt.ongoings.map(_.actionID).mkString(",")}
       |${evt.snapshot.show}
       |""".stripMargin

  private def metricReset(evt: MetricReset): String =
    s"""${evt.resetType.show}
       |${serviceEvent(evt)}
       |${evt.snapshot.show}
       |""".stripMargin

  private def passThrough(evt: PassThrough): String =
    s"""Pass Through
       |${instantEvent(evt)}
       |  Message:${evt.value.noSpaces}
       |""".stripMargin

  private def instantAlert(evt: InstantAlert): String =
    s"""Service Alert
       |${instantEvent(evt)}
       |  Alert:${evt.message}
       |""".stripMargin

  private def actionStart(evt: ActionStart): String =
    s"""Action Start
       |${actionEvent(evt)}
       |""".stripMargin

  private def actionRetrying(evt: ActionRetry): String =
    s"""Action Retrying
       |${actionEvent(evt)}
       |  Took:${fmt.format(evt.took)}
       |  StackTrace:${evt.error.stackTrace}
       |""".stripMargin

  private def actionFailed(evt: ActionFail): String =
    s"""Action Failed
       |${actionEvent(evt)}
       |  Took:${fmt.format(evt.took)}
       |  StackTrace:${evt.error.stackTrace}
       |  ${evt.notes.value}
       |""".stripMargin

  private def actionSucced(evt: ActionSucc): String =
    s"""Action Succed
       |${actionEvent(evt)}
       |  Took:${fmt.format(evt.took)}
       |  ${evt.notes.value}
       |""".stripMargin

  def apply[F[_]: Applicative]: Translator[F, String] =
    Translator
      .empty[F, String]
      .withServiceStart(serviceStarted)
      .withServiceStop(serviceStopped)
      .withServicePanic(servicePanic)
      .withMetricsReport(metricReport)
      .withMetricsReset(metricReset)
      .withPassThrough(passThrough)
      .withInstantAlert(instantAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying)
      .withActionFail(actionFailed)
      .withActionSucc(actionSucced)

}
