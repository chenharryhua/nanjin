package com.github.chenharryhua.nanjin.guard.translators

import cats.Applicative
import com.github.chenharryhua.nanjin.guard.event.*

// TODO
private[guard] object PrettyTextTranslator {

  private def serviceStarted(ss: ServiceStart): String =
    s"""
       |App Name: ${ss.serviceParams.taskParams.appName}
       |Service Name: ${ss.name.value}
       |Up Time: ${fmt.format(ss.upTime)}
       |Time Zone: ${ss.serviceParams.taskParams.zoneId}
       |Host: ${ss.serviceParams.taskParams.hostName}
       |""".stripMargin

  private def servicePanic[F[_]: Applicative](sp: ServicePanic): String =
    s"""
       |Service Name: ${sp.name.value}
       |Host: ${sp.serviceParams.taskParams.hostName}
       |Up Time: ${fmt.format(sp.upTime)}
       |Policy: ${sp.serviceParams.retry.policy[F].show}
       |
       |""".stripMargin

  private def serviceStopped(ss: ServiceStop): String =
    s"""
       |${ss.show}
       |""".stripMargin

  private def metricsReport(mr: MetricsReport): String =
    s"""
       |${mr.show}
       |""".stripMargin

  private def metricsReset(ms: MetricsReset): String =
    s"""
       |${ms.show}
       |""".stripMargin

  private def passThrough(pt: PassThrough): String =
    s"""
       |
       |${pt.show}
       |""".stripMargin

  private def serviceAlert(sa: ServiceAlert): String =
    s"""
       |${sa.show}
       |""".stripMargin

  private def actionStart(as: ActionStart): String =
    s"""
       |${as.show}
       |""".stripMargin

  private def actionRetrying(ar: ActionRetry): String =
    s"""
       |${ar.show}
       |""".stripMargin

  private def actionFailed[F[_]: Applicative](af: ActionFail): String =
    s"""
       |${af.show}
       |""".stripMargin

  private def actionSucced(as: ActionSucc): String =
    s"""
       |${as.show}
       |""".stripMargin

  def apply[F[_]: Applicative]: Translator[F, String] =
    Translator
      .empty[F, String]
      .withServiceStart(serviceStarted)
      .withServiceStop(serviceStopped)
      .withServicePanic(servicePanic[F])
      .withMetricsReport(metricsReport)
      .withMetricsReset(metricsReset)
      .withPassThrough(passThrough)
      .withServiceAlert(serviceAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying)
      .withActionFail(actionFailed[F])
      .withActionSucc(actionSucced)

}
