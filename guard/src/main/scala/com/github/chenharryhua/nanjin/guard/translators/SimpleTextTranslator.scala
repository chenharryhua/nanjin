package com.github.chenharryhua.nanjin.guard.translators

import cats.Applicative
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.*

private[translators] object SimpleTextTranslator {
  private def coloring(msg: String): Coloring = new Coloring({
    case ColorScheme.GoodColor  => msg
    case ColorScheme.InfoColor  => msg
    case ColorScheme.WarnColor  => Console.CYAN + msg + Console.RESET
    case ColorScheme.ErrorColor => Console.YELLOW + msg + Console.RESET
  })

  private def serviceEvent(se: ServiceEvent): String = {
    val host: String = se.serviceParams.taskParams.hostName.value
    val sn: String   = se.serviceParams.serviceName.value
    s"  Host:$host, ServiceID:${se.serviceID.show}, ServiceName:$sn"
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
    s"""${coloring("Service (Re)Started")(evt)}
       |${serviceEvent(evt)}
       |""".stripMargin

  private def servicePanic(evt: ServicePanic): String =
    s"""${coloring("Service Panic")(evt)}
       |${serviceEvent(evt)}
       |  ${upcomingRestartTimeInterpretation(evt)}
       |  ErrorID:${evt.error.uuid.show}
       |  StackTrace:${evt.error.stackTrace}
       |""".stripMargin

  private def serviceStopped(evt: ServiceStop): String =
    s"""${coloring("Service Stopped")(evt)}
       |${serviceEvent(evt)}
       |  StackTrace:${evt.cause.show}
       |""".stripMargin

  private def metricReport(evt: MetricReport): String =
    s"""${coloring(evt.reportType.show)(evt)}
       |${serviceEvent(evt)}
       |  ${upcomingRestartTimeInterpretation(evt)}
       |  Ongoings:${evt.ongoings.map(_.actionID).mkString(",")}
       |${evt.snapshot.show}
       |""".stripMargin

  private def metricReset(evt: MetricReset): String =
    s"""${coloring(evt.resetType.show)(evt)}
       |${serviceEvent(evt)}
       |${evt.snapshot.show}
       |""".stripMargin

  private def passThrough(evt: PassThrough): String =
    s"""${coloring("Pass Through")(evt)}
       |${instantEvent(evt)}
       |  Message:${evt.value.noSpaces}
       |""".stripMargin

  private def instantAlert(evt: InstantAlert): String =
    s"""${coloring("Service Alert")(evt)}
       |${instantEvent(evt)}
       |  Alert:${evt.message}
       |""".stripMargin

  private def actionStart(evt: ActionStart): String =
    s"""${coloring("Action Start")(evt)}
       |${actionEvent(evt)}
       |""".stripMargin

  private def actionRetrying(evt: ActionRetry): String =
    s"""${coloring("Action Retrying")(evt)}
       |${actionEvent(evt)}
       |  Took:${fmt.format(evt.took)}
       |  StackTrace:${evt.error.stackTrace}
       |""".stripMargin

  private def actionFailed(evt: ActionFail): String =
    s"""${coloring("Action Failed")(evt)}
       |${actionEvent(evt)}
       |  Took:${fmt.format(evt.took)}
       |  StackTrace:${evt.error.stackTrace}
       |  ${evt.notes.value}
       |""".stripMargin

  private def actionSucced(evt: ActionSucc): String =
    s"""${coloring("Action Succed")(evt)}
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
