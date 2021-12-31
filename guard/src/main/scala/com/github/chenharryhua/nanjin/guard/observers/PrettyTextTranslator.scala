package com.github.chenharryhua.nanjin.guard.observers

import cats.Applicative
import com.github.chenharryhua.nanjin.guard.event.*

object PrettyTextTranslator {

  private def serviceStarted(ss: ServiceStart): String =
    s"""
       |App Name: ${ss.serviceInfo.serviceParams.taskParams.appName}
       |Service Name: ${ss.name.value}
       |Up Time: ${tookStr(ss.serviceInfo.launchTime, ss.timestamp)}
       |Time Zone: ${ss.serviceInfo.serviceParams.taskParams.zoneId}
       |Host: ${ss.serviceInfo.serviceParams.taskParams.hostName}
       |""".stripMargin

  private def servicePanic[F[_]: Applicative](sp: ServicePanic): String =
    s"""
       |Service Name: ${sp.name.value}
       |Host: ${sp.serviceInfo.serviceParams.taskParams.hostName}
       |Up Time: ${tookStr(sp.serviceInfo.launchTime, sp.timestamp)}
       |Policy: ${sp.serviceInfo.serviceParams.retry.policy[F].show}
       |
       |""".stripMargin

  private def serviceStopped(ss: ServiceStop): String = ""

  private def metricsReport(mr: MetricsReport): String = ""

  private def metricsReset(ms: MetricsReset): String = ""

  private def passThrough(pt: PassThrough): String = ""

  private def serviceAlert(sa: ServiceAlert): String = ""

  private def actionStart(as: ActionStart): String = ""

  private def actionRetrying(ar: ActionRetry): String = ""

  private def actionFailed[F[_]: Applicative](af: ActionFail): String = ""

  private def actionSucced(as: ActionSucc): String = ""

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
