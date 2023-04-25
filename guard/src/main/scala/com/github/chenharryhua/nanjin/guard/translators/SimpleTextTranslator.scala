package com.github.chenharryhua.nanjin.guard.translators

import cats.syntax.all.*
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.guard.event.{NJError, NJEvent}
import io.circe.syntax.EncoderOps

private object SimpleTextTranslator {
  import NJEvent.*
  import textConstant.*

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
    val host: String      = s"$CONSTANT_HOST:${se.serviceParams.taskParams.hostName.value}"
    val sn: String        = s"$CONSTANT_SERVICE:${se.serviceParams.serviceName}"
    val tn: String        = s"$CONSTANT_TASK:${se.serviceParams.taskParams.taskName}"
    val serviceId: String = s"SID:${se.serviceParams.serviceId.show.takeRight(12)}"
    val uptime: String    = s"$CONSTANT_UPTIME:${fmt.format(se.upTime)}"
    s"$sn, $tn, $host, $serviceId, $uptime"
  }

  private def errorStr(err: NJError): String = s"Cause:${err.stackTrace}"

  private def actionEvent(ae: ActionEvent): String = {
    val id         = s"$CONSTANT_ACTION_ID:${ae.actionId}"
    val trace      = s"$CONSTANT_TRACE_ID:${ae.traceId}"
    val importance = s"$CONSTANT_IMPORTANCE:${ae.actionParams.importance.entryName}"
    val strategy   = s"$CONSTANT_STRATEGY:${ae.actionParams.publishStrategy.entryName}"
    s"""  ${serviceEvent(ae)}
       |  $id, $trace, $importance, $strategy""".stripMargin
  }

  private def serviceStarted(evt: ServiceStart): String =
    s"""${coloring(evt)}
       |  ${serviceEvent(evt)}
       |${evt.serviceParams.asJson.spaces2}
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
       |${yamlSnapshot(evt.snapshot, evt.serviceParams.metricParams)}
       |""".stripMargin

  private def metricReset(evt: MetricReset): String =
    s"""${coloring(evt)}
       |  ${serviceEvent(evt)}
       |${yamlSnapshot(evt.snapshot, evt.serviceParams.metricParams)}
       |""".stripMargin

  private def serviceAlert(evt: ServiceAlert): String =
    s"""${coloring(evt)}
       |  ${serviceEvent(evt)}
       |${evt.message.spaces2}
       |""".stripMargin

  private def actionStart(evt: ActionStart): String =
    s"""${coloring(evt)}
       |${actionEvent(evt)}
       |${evt.notes.fold("")(_.spaces2)}
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
       |  ${errorStr(evt.error)}
       |${evt.notes.fold("")(_.spaces2)}
       |""".stripMargin

  private def actionComplete(evt: ActionComplete): String =
    s"""${coloring(evt)}
       |${actionEvent(evt)}
       |  $CONSTANT_TOOK:${fmt.format(evt.took)}
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
      .withActionComplete(actionComplete)

}
