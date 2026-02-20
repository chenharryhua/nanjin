package com.github.chenharryhua.nanjin.guard.service

import cats.Applicative
import cats.syntax.show.showInterpolator
import com.github.chenharryhua.nanjin.guard.config.Attribute
import com.github.chenharryhua.nanjin.guard.event.{Event, Index, Took}
import com.github.chenharryhua.nanjin.guard.translator.{textHelper, Translator}

private object SimpleTextTranslator {
  import Event.*

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
    val idx = Attribute(Index(evt.tick.index)).labelledText
    val snz = Attribute(Took(evt.tick.snooze)).labelledText
    s"""|
        |  ${service_event(evt)}
        |  $idx, $snz
        |${evt.serviceParams.simpleJson.spaces2}
        |""".stripMargin
  }

  private def service_panic(evt: ServicePanic): String = {
    val idx = Attribute(Index(evt.tick.index)).labelledText
    val act = Attribute(Took(evt.tick.active)).labelledText
    val policy = Attribute(evt.serviceParams.servicePolicies.restart.policy).labelledText

    show"""|
           |  ${service_event(evt)}
           |  $policy
           |  ${textHelper.panicText(evt)}
           |  $idx, $act
           |${Attribute(evt.stackTrace).labelledText}
           |""".stripMargin
  }

  private def service_stop(evt: ServiceStop): String = {
    val policy = Attribute(evt.serviceParams.servicePolicies.restart.policy).labelledText

    show"""|
           |  ${service_event(evt)}
           |  $policy
           |${Attribute(evt.cause).labelledText}
           |""".stripMargin
  }

  private def metrics_event(evt: MetricsEvent): String = {
    val policy = Attribute(evt.serviceParams.servicePolicies.metricsReport).labelledText
    val index = Attribute(evt.index).labelledText

    s"""|
        |  ${service_event(evt)}
        |  $index, $policy, ${Attribute(evt.took).labelledText}
        |${textHelper.yamlMetrics(evt.snapshot)}
        |""".stripMargin
  }

  private def metrics_report(evt: MetricsReport): String =
    metrics_event(evt)

  private def metrics_reset(evt: MetricsReset): String =
    metrics_event(evt)

  private def service_message(evt: ServiceMessage): String = {
    val correlation = Attribute(evt.correlation).labelledText
    val domain = Attribute(evt.domain).labelledText
    s"""|
        |  ${service_event(evt)}
        |  $domain, $correlation
        |${evt.message.spaces2}
        |${evt.stackTrace.fold("")(Attribute(_).labelledText)}
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
