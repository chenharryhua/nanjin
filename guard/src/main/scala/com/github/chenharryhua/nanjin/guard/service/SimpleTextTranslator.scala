package com.github.chenharryhua.nanjin.guard.service

import cats.Applicative
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.config.Attribute
import com.github.chenharryhua.nanjin.guard.event.{Event, Took}
import com.github.chenharryhua.nanjin.guard.translator.{panicText, SnapshotPolyglot, Translator}

private object SimpleTextTranslator {
  import Event.*

  private case class Index(value: Long)

  private def service_event(se: Event): String = {
    val host: String = Attribute(se.serviceParams.host).labelledText
    val sn: String = Attribute(se.serviceParams.serviceName).labelledText
    val tn: String = Attribute(se.serviceParams.taskName).labelledText
    val sid: String = Attribute(se.serviceParams.serviceId).labelledText
    val uptime: String = Attribute(se.upTime).labelledText
    s"""|$sn, $tn, $uptime
        |  $host
        |  $sid""".stripMargin
  }

  private def service_start(evt: ServiceStart): String = {
    val idx = Attribute(Index(evt.tick.index)).map(_.value).labelledText
    val snz = Attribute(Took(evt.tick.snooze)).labelledText
    s"""|
        |  ${service_event(evt)}
        |  $idx, $snz
        |${evt.serviceParams.simpleJson.spaces2}
        |""".stripMargin
  }

  private def service_panic(evt: ServicePanic): String = {
    val idx = Attribute(Index(evt.tick.index)).map(_.value).labelledText
    val act = Attribute(Took(evt.tick.active)).labelledText
    val policy = Attribute(evt.serviceParams.servicePolicies.restart.policy).labelledText

    s"""|
        |  ${service_event(evt)}
        |  $policy
        |  ${panicText(evt)}
        |  $idx, $act
        |${Attribute(evt.stackTrace).labelledText}
        |""".stripMargin
  }

  private def service_stop(evt: ServiceStop): String = {
    val policy = Attribute(evt.serviceParams.servicePolicies.restart.policy).labelledText

    s"""|
        |  ${service_event(evt)}
        |  $policy
        |${Attribute(evt.cause).labelledText}
        |""".stripMargin
  }

  private def metrics_event(evt: MetricsEvent, policy: Policy): String = {
    val policy_text = Attribute(policy).labelledText
    val index = Attribute(evt.index).labelledText

    s"""|
        |  ${service_event(evt)}
        |  $index, $policy_text, ${Attribute(evt.took).labelledText}
        |${new SnapshotPolyglot(evt.snapshot).toYaml}
        |""".stripMargin
  }

  private def metrics_report(evt: MetricsReport): String =
    metrics_event(evt, evt.serviceParams.servicePolicies.metricsReport)

  private def metrics_reset(evt: MetricsReset): String =
    metrics_event(evt, evt.serviceParams.servicePolicies.metricsReset)

  private def service_message(evt: ServiceMessage): String = {
    val correlation = Attribute(evt.correlation).labelledText
    val domain = Attribute(evt.domain).labelledText
    val message = evt.message.value.spaces2
    s"""|
        |  ${service_event(evt)}
        |  $domain, $correlation
        |${evt.stackTrace.fold(message) { st =>
         s"""|$message
             |${Attribute(st).labelledText}""".stripMargin
       }}
        |""".stripMargin
  }

  def apply[F[_]: Applicative]: Translator[F, String] =
    Translator
      .empty[F, String]
      .withServiceStart(service_start)
      .withServiceStop(service_stop)
      .withServicePanic(service_panic)
      .withMetricsReport(metrics_report)
      .withMetricsReset(metrics_reset)
      .withServiceMessage(service_message)
}
