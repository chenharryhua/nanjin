package com.github.chenharryhua.nanjin.guard.translators

import cats.syntax.all.*
import cats.{Applicative, Eval}
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
    val host: String      = se.serviceParams.taskParams.hostName.value
    val sn: String        = se.serviceParams.serviceName.value
    val tn: String        = se.serviceParams.taskParams.taskName.value
    val serviceId: String = se.serviceParams.serviceId.show.takeRight(12)
    s"Service:$sn, Task:$tn, Host:$host, SID:$serviceId, UpTime:${fmt.format(se.upTime)}"
  }

  private def errorStr(err: NJError): String = s"Cause:${err.stackTrace}"

  private def actionEvent(ae: ActionEvent): String =
    s"""  ${serviceEvent(ae)}
       |  Importance:${ae.actionParams.importance.show}, ActionID:${ae.actionId}, TraceID:${ae.traceId}""".stripMargin

  private def serviceStarted(evt: ServiceStart): String =
    s"""${coloring(evt.title)(evt)}
       |  ${serviceEvent(evt)}
       |  brief:${evt.serviceParams.brief.noSpaces}
       |""".stripMargin

  private def servicePanic(evt: ServicePanic): String = {
    val (time, dur) = localTimeAndDurationStr(evt.timestamp, evt.restartTime)
    val msg         = s"The service experienced a panic. Restart was scheduled at $time, roughly in $dur."
    s"""${coloring(evt.title)(evt)}
       |  ${serviceEvent(evt)}
       |  $msg
       |  Policy:${evt.serviceParams.restartPolicy}
       |  ${errorStr(evt.error)}
       |""".stripMargin
  }

  private def serviceStopped(evt: ServiceStop): String =
    s"""${coloring(evt.title)(evt)}
       |  ${serviceEvent(evt)}
       |  Policy:${evt.serviceParams.restartPolicy}
       |  Cause:${evt.cause.show}
       |""".stripMargin

  private def metricReport(evt: MetricReport): String =
    s"""${coloring(evt.title)(evt)}
       |  ${serviceEvent(evt)}
       |${showSnapshot(evt.serviceParams, evt.snapshot)}
       |""".stripMargin

  private def metricReset(evt: MetricReset): String =
    s"""${coloring(evt.title)(evt)}
       |  ${serviceEvent(evt)}
       |${showSnapshot(evt.serviceParams, evt.snapshot)}
       |""".stripMargin

  private def passThrough(evt: PassThrough): String =
    s"""${coloring(instantEventTitle(evt))(evt)}
       |  ${serviceEvent(evt)}
       |  Message:${evt.value.noSpaces}
       |""".stripMargin

  private def instantAlert(evt: InstantAlert): String =
    s"""${coloring(instantEventTitle(evt))(evt)}
       |  ${serviceEvent(evt)}
       |  Alert:${evt.message}
       |""".stripMargin

  private def actionStart(evt: ActionStart): String =
    s"""${coloring(actionTitle(evt))(evt)}
       |${actionEvent(evt)}
       |""".stripMargin

  private def actionRetry(evt: ActionRetry): String =
    s"""${coloring(actionTitle(evt))(evt)}
       |${actionEvent(evt)}
       |  Took:${fmt.format(evt.tookSoFar)}
       |  Policy:${evt.actionParams.retryPolicy}
       |  ${errorStr(evt.error)}
       |""".stripMargin

  private def actionFail(evt: ActionFail): String =
    s"""${coloring(actionTitle(evt))(evt)}
       |${actionEvent(evt)}
       |  Took:${fmt.format(evt.took)}
       |  Policy:${evt.actionParams.retryPolicy}
       |  Notes:${evt.output.noSpaces}
       |  ${errorStr(evt.error)}
       |""".stripMargin

  private def actionComplete(evt: ActionComplete): String =
    s"""${coloring(actionTitle(evt))(evt)}
       |${actionEvent(evt)}
       |  Took:${fmt.format(evt.took)}
       |  Result:${evt.output.noSpaces}
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
      .withActionRetry(actionRetry)
      .withActionFail(actionFail)
      .withActionComplete(actionComplete)

}
