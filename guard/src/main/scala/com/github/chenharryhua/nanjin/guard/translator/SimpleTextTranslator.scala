package com.github.chenharryhua.nanjin.guard.translator

import cats.Applicative
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.guard.event.{Event, Index, Took}

object SimpleTextTranslator {
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
    val idx = s"index:${evt.tick.index}"
    val snz = Attribute(Took(evt.tick.snooze)).labelledText

    s"""|
        |  ${service_event(evt)}
        |  $idx, $snz
        |${interpretServiceParams(evt.serviceParams).spaces2}
        |""".stripMargin
  }

  private def service_panic(evt: ServicePanic): String = {
    val idx = s"index:${evt.tick.index}"
    val act = Attribute(Took(evt.tick.active)).labelledText
    val policy = Attribute(evt.serviceParams.servicePolicies.restart.policy).labelledText

    s"""|
        |  ${service_event(evt)}
        |  $idx, $act
        |  $policy
        |${panicText(evt)}
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

  private def metrics_event(evt: MetricsEvent): String = {
    val policy = Attribute(evt.kind.policy).labelledText
    val index = Attribute(evt.index).map {
      case ad @ Index.Adhoc(_)  => s"${evt.kind.show}-${ad.productPrefix}"
      case Index.Periodic(tick) => s"${evt.kind.show}-${tick.index}"
    }.labelledText
    val took = Attribute(evt.took).labelledText

    s"""|
        |  ${service_event(evt)}
        |  $policy
        |  $index, $took
        |${new SnapshotPolyglot(evt.snapshot).toYaml}
        |""".stripMargin
  }

  private def reported_event(evt: ReportedEvent): String = {
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
      .withMetricsEvent(metrics_event)
      .withReportedEvent(reported_event)
}
