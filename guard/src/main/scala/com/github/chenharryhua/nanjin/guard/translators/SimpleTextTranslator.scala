package com.github.chenharryhua.nanjin.guard.translators

import cats.{Applicative, Eval}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.{NJError, NJEvent}

private object SimpleTextTranslator {
  import NJEvent.*
  private def coloring(msg: String)(evt: NJEvent): String = ColorScheme
    .decorate(evt)
    .run {
      case ColorScheme.GoodColor  => Eval.now(Console.GREEN + msg + Console.RESET)
      case ColorScheme.InfoColor  => Eval.now(msg)
      case ColorScheme.WarnColor  => Eval.now(Console.YELLOW + msg + Console.RESET)
      case ColorScheme.ErrorColor => Eval.now(Console.RED + msg + Console.RESET)
    }
    .value

  private def serviceEvent(se: ServiceEvent): String = {
    val host: String = se.serviceParams.taskParams.hostName.value
    val sn: String   = se.serviceParams.serviceName.value
    val tn: String   = se.serviceParams.taskParams.taskName.value
    s"Task:$tn, Service:$sn, Host:$host"
  }

  private def instantEvent(ie: InstantEvent): String =
    s"""|  ${serviceEvent(ie)}
        |  Name:${ie.digested.metricRepr}""".stripMargin

  private def errorStr(err: NJError): String = s"Cause:${err.stackTrace}"

  private def actionEvent(ae: ActionEvent): String =
    s"""  ${serviceEvent(ae)}
       |  Name:${ae.digested.metricRepr}, ID:${ae.actionId}, TraceID:${ae.traceId}""".stripMargin

  private def serviceStarted(evt: ServiceStart): String =
    s"""${coloring(evt.title)(evt)}
       |  ${serviceEvent(evt)}
       |  ${evt.serviceParams.brief}
       |""".stripMargin

  private def servicePanic(evt: ServicePanic): String =
    s"""${coloring(evt.title)(evt)}
       |  ${serviceEvent(evt)}
       |  ${upcomingRestartTimeInterpretation(evt)}
       |  ${errorStr(evt.error)}
       |""".stripMargin

  private def serviceStopped(evt: ServiceStop): String =
    s"""${coloring(evt.title)(evt)}
       |  ${serviceEvent(evt)}
       |  Cause:${evt.cause.show}
       |""".stripMargin

  private def metricReport(evt: MetricReport): String =
    s"""${coloring(evt.title)(evt)}
       |  ${serviceEvent(evt)}
       |  ${upcomingRestartTimeInterpretation(evt)}
       |${evt.snapshot.show}
       |""".stripMargin

  private def metricReset(evt: MetricReset): String =
    s"""${coloring(evt.title)(evt)}
       |  ${serviceEvent(evt)}
       |${evt.snapshot.show}
       |""".stripMargin

  private def passThrough(evt: PassThrough): String =
    s"""${coloring(evt.title)(evt)}
       |${instantEvent(evt)}
       |  Message:${evt.value.noSpaces}
       |""".stripMargin

  private def instantAlert(evt: InstantAlert): String =
    s"""${coloring(evt.title)(evt)}
       |${instantEvent(evt)}
       |  Alert:${evt.message}
       |""".stripMargin

  private def actionStart(evt: ActionStart): String =
    s"""${coloring(evt.title)(evt)}
       |${actionEvent(evt)}
       |  Input:${evt.input.noSpaces}
       |""".stripMargin

  private def actionRetrying(evt: ActionRetry): String =
    s"""${coloring(evt.title)(evt)}
       |${actionEvent(evt)}
       |  Took:${fmt.format(evt.took)}
       |  ${errorStr(evt.error)}
       |""".stripMargin

  private def actionFailed(evt: ActionFail): String =
    s"""${coloring(evt.title)(evt)}
       |${actionEvent(evt)}
       |  Took:${fmt.format(evt.took)}
       |  Input:${evt.input.noSpaces}
       |  ${errorStr(evt.error)}
       |""".stripMargin

  private def actionSucced(evt: ActionSucc): String =
    s"""${coloring(evt.title)(evt)}
       |${actionEvent(evt)}
       |  Took:${fmt.format(evt.took)}
       |  Output:${evt.output.noSpaces}
       |""".stripMargin

  private def rootSpanStart(evt: RootSpanStart): String =
    s"""${coloring(evt.title)(evt)}
       |  ${serviceEvent(evt)}
       |  TraceID:${evt.internalTraceId}
       |""".stripMargin

  private def rootSpanFinish(evt: RootSpanFinish): String =
    s"""${coloring(evt.title)(evt)}
       |  ${serviceEvent(evt)}
       |  Name:${evt.rootSpanName}, TraceID:${evt.internalTraceId}
       |  Result:${evt.result.map(_.message).getOrElse("Succed")}
       |""".stripMargin

  def apply[F[_]: Applicative]: Translator[F, String] =
    Translator
      .empty[F, String]
      .withServiceStart(serviceStarted)
      .withServiceStop(serviceStopped)
      .withServicePanic(servicePanic)
      .withMetricReport(metricReport)
      .withMetricReset(metricReset)
      .withPassThrough(passThrough)
      .withInstantAlert(instantAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying)
      .withActionFail(actionFailed)
      .withActionSucc(actionSucced)
      .withRootSpanStart(rootSpanStart)
      .withRootSpanFinish(rootSpanFinish)

}
