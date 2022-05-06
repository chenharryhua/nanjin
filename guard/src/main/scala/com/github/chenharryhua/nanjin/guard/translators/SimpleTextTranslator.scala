package com.github.chenharryhua.nanjin.guard.translators

import cats.Applicative
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.*

private[translators] object SimpleTextTranslator {

  private def serviceEvent(se: ServiceEvent): String = {
    val host: String = se.serviceParams.taskParams.hostName.value
    val sn: String   = se.serviceParams.serviceName.value
    s"  Service: $sn, Host: $host, ID: ${se.serviceID.show}, Up-Time: ${fmt.format(se.upTime)}"
  }

  private def instantEvent(ie: InstantEvent): String = {
    val host: String = ie.serviceParams.taskParams.hostName.value
    val sn: String   = ie.serviceParams.serviceName.value
    s"""|  Service: $sn, Host: $host, ID: ${ie.serviceID.show}
        |  Name: ${ie.metricName.metricRepr}""".stripMargin
  }

  private def actionEvent(ae: ActionEvent): String = {
    val host: String = ae.serviceParams.taskParams.hostName.value
    val sn: String   = ae.serviceParams.serviceName.value
    s"  Service: $sn, Host: $host, ID: ${ae.serviceID.show}"
  }

  private def serviceStarted(evt: ServiceStart): String =
    s"""Service (Re)Started
       |${serviceEvent(evt)}
       |""".stripMargin

  private def servicePanic(evt: ServicePanic): String =
    s"""Service Panic
       |${serviceEvent(evt)}
       |  Restarts: ${evt.retryDetails.retriesSoFar}
       |""".stripMargin

  private def serviceStopped(evt: ServiceStop): String =
    s"""Service Stopped
       |${serviceEvent(evt)}
       |  Cause: ${evt.cause.show}
       |""".stripMargin

  private def metricReport(evt: MetricReport): String =
    s"""${evt.reportType.show}
       |${serviceEvent(evt)}
       |  On Goings: ${evt.ongoings.map(_.actionID).mkString(",")}
       |  Metrics:
       |${evt.snapshot.show}
       |""".stripMargin

  private def metricReset(evt: MetricReset): String =
    s"""${evt.resetType.show}
       |${serviceEvent(evt)}
       |  Metrics:
       |${evt.snapshot.show}
       |
       |""".stripMargin

  private def passThrough(evt: PassThrough): String =
    s"""Pass Through
       |${instantEvent(evt)}
       |  Message: ${evt.value.noSpaces}
       |""".stripMargin

  private def instantAlert(evt: InstantAlert): String =
    s"""Service Alert
       |${instantEvent(evt)}
       |  Alert: ${evt.message}
       |""".stripMargin

  private def actionStart(evt: ActionStart): String =
    s"""${evt.actionInfo.actionParams.startTitle}
       |${actionEvent(evt)}
       |  ${evt.actionParams.catalog} ID: ${evt.actionID.show}
       |""".stripMargin

  private def actionRetrying(evt: ActionRetry): String =
    s"""${evt.actionInfo.actionParams.retryTitle}
       |${actionEvent(evt)}
       |  ${evt.actionParams.catalog} ID: ${evt.actionID.show}, Took: ${fmt.format(evt.took)}
       |""".stripMargin

  private def actionFailed(evt: ActionFail): String =
    s"""${evt.actionInfo.actionParams.failedTitle}
       |${actionEvent(evt)}
       |  ${evt.actionParams.catalog} ID: ${evt.actionID.show}, Took: ${fmt.format(evt.took)}, ${evt.notes.value}
       |""".stripMargin

  private def actionSucced(evt: ActionSucc): String =
    s"""${evt.actionInfo.actionParams.succedTitle}
       |${actionEvent(evt)}
       |  ${evt.actionParams.catalog} ID: ${evt.actionID.show}, Took: ${fmt.format(evt.took)}, ${evt.notes.value}
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
