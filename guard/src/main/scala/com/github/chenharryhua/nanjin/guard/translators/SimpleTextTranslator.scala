package com.github.chenharryhua.nanjin.guard.translators

import cats.syntax.all.*
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.guard.event.{NJError, NJEvent}

private object SimpleTextTranslator {
  import NJEvent.*
  private def coloring(evt: NJEvent): String = {
    val msg: String = eventTitle(evt)
    ColorScheme
      .decorate(evt)
      .run {
        case ColorScheme.GoodColor  => Eval.now(Console.GREEN + msg + Console.RESET)
        case ColorScheme.InfoColor  => Eval.now(msg)
        case ColorScheme.WarnColor  => Eval.now(Console.YELLOW + msg + Console.RESET)
        case ColorScheme.ErrorColor => Eval.now(Console.RED + msg + Console.RESET)
      }
      .value
  }

  private def serviceEvent(se: NJEvent): String = {
    val host: String      = se.serviceParams.taskParams.hostName.value
    val sn: String        = se.serviceParams.serviceName.value
    val tn: String        = se.serviceParams.taskParams.taskName.value
    val serviceId: String = se.serviceParams.serviceId.show.takeRight(12)
    val uptime: String    = fmt.format(se.upTime)
    s"$CONSTANT_SERVICE:$sn, $CONSTANT_TASK:$tn, $CONSTANT_HOST:$host, SID:$serviceId, $CONSTANT_UPTIME:$uptime"
  }

  private def errorStr(err: NJError): String = s"Cause:${err.stackTrace}"

  private def actionEvent(ae: ActionEvent): String =
    s"""  ${serviceEvent(ae)}
       |  $CONSTANT_IMPORTANCE:${ae.actionParams.importance.show}, $CONSTANT_ACTION_ID:${ae.actionId}, $CONSTANT_TRACE_ID:${ae.traceId}""".stripMargin

  private def serviceStarted(evt: ServiceStart): String =
    s"""${coloring(evt)}
       |  ${serviceEvent(evt)}
       |  $CONSTANT_BRIEF:${evt.serviceParams.brief.noSpaces}
       |""".stripMargin

  private def servicePanic(evt: ServicePanic): String = {
    val (time, dur) = localTimeAndDurationStr(evt.timestamp, evt.restartTime)
    val msg         = s"The service experienced a panic. Restart was scheduled at $time, roughly in $dur."
    s"""${coloring(evt)}
       |  ${serviceEvent(evt)}
       |  $msg
       |  $CONSTANT_POLICY:${evt.serviceParams.restartPolicy}
       |  ${errorStr(evt.error)}
       |""".stripMargin
  }

  private def serviceStopped(evt: ServiceStop): String =
    s"""${coloring(evt)}
       |  ${serviceEvent(evt)}
       |  $CONSTANT_POLICY:${evt.serviceParams.restartPolicy}
       |  $CONSTANT_CAUSE:${evt.cause.show}
       |""".stripMargin

  private def metricReport(evt: MetricReport): String =
    s"""${coloring(evt)}
       |  ${serviceEvent(evt)}
       |${new SnapshotJson(evt.snapshot).toPrettyJson(evt.serviceParams.metricParams).spaces2}
       |""".stripMargin

  private def metricReset(evt: MetricReset): String =
    s"""${coloring(evt)}
       |  ${serviceEvent(evt)}
       |${new SnapshotJson(evt.snapshot).toPrettyJson(evt.serviceParams.metricParams).spaces2}
       |""".stripMargin

  private def serviceAlert(evt: ServiceAlert): String =
    s"""${coloring(evt)}
       |  ${serviceEvent(evt)}
       |  ${evt.alertLevel.show}:${evt.message}
       |""".stripMargin

  private def actionStart(evt: ActionStart): String =
    s"""${coloring(evt)}
       |${actionEvent(evt)}
       |""".stripMargin

  private def actionRetry(evt: ActionRetry): String =
    s"""${coloring(evt)}
       |${actionEvent(evt)}
       |  $CONSTANT_TOOK:${fmt.format(evt.tookSoFar)}
       |  $CONSTANT_POLICY:${evt.actionParams.retryPolicy}
       |  ${errorStr(evt.error)}
       |""".stripMargin

  private def actionFail(evt: ActionFail): String =
    s"""${coloring(evt)}
       |${actionEvent(evt)}
       |  $CONSTANT_TOOK:${fmt.format(evt.took)}
       |  $CONSTANT_POLICY:${evt.actionParams.retryPolicy}
       |  $CONSTANT_NOTES:${evt.output.noSpaces}
       |  ${errorStr(evt.error)}
       |""".stripMargin

  private def actionComplete(evt: ActionComplete): String =
    s"""${coloring(evt)}
       |${actionEvent(evt)}
       |  $CONSTANT_TOOK:${fmt.format(evt.took)}
       |  $CONSTANT_RESULT:${evt.output.noSpaces}
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
      .withActionComplete(actionComplete)

}
