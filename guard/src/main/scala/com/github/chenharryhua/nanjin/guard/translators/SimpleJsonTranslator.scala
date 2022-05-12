package com.github.chenharryhua.nanjin.guard.translators

import cats.Applicative
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.guard.event.*
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.circe.syntax.*

private object SimpleJsonTranslator {

  private def serviceStarted(evt: ServiceStart): Json =
    json"""
          { 
            "event": "ServiceStart",
            "params": ${evt.serviceParams.asJson}
          }
          """

  private def servicePanic(evt: ServicePanic): Json =
    json"""
          {
            "event": "ServicePanic",
            "serviceName" : ${evt.serviceParams.serviceName.value},
            "cause" : ${evt.error.stackTrace},
            "serviceID": ${evt.serviceParams.serviceID}
          }
          """

  private def serviceStopped(evt: ServiceStop): Json =
    json"""
          {
            "event": "ServiceStop",
            "serviceName" : ${evt.serviceParams.serviceName.value},
            "exitCode": ${evt.cause.exitCode},
            "cause": ${evt.cause.show},
            "serviceID": ${evt.serviceParams.serviceID}
          }
          """

  private def metricReport(evt: MetricReport): Json = {
    val index: Option[Long] = evt.reportType match {
      case MetricReportType.Adhoc(_)            => None
      case MetricReportType.Scheduled(_, index) => Some(index)
    }
    json"""
          {
            "event": "MetricReport",
            "serviceName" : ${evt.serviceParams.serviceName.value},
            "index": $index,
            "isUp": ${evt.isUp},
            "ongoings": ${evt.ongoings.map(_.actionID)},            
            "metrics": ${evt.snapshot.asJson},
            "serviceID": ${evt.serviceParams.serviceID}
          }
          """
  }

  private def metricReset(evt: MetricReset): Json =
    json"""
          {
            "event": "MetricReset",
            "serviceName" : ${evt.serviceParams.serviceName.value},
            "metrics": ${evt.snapshot.asJson},
            "serviceID": ${evt.serviceParams.serviceID}
          }
          """

  private def passThrough(evt: PassThrough): Json =
    json"""
          {
            "event": "PassThrough",
            "name": ${evt.metricName.origin},
            "isError": ${evt.isError},
            "value": ${evt.value},
            "digest": ${evt.metricName.digest},
            "serviceID": ${evt.serviceParams.serviceID}
          }
          """

  private def instantAlert(evt: InstantAlert): Json =
    json"""
          {       
            "event": "InstantAlert",
            "name": ${evt.metricName.origin},
            "importance": ${evt.importance.value},
            "message": ${evt.message},
            "digest": ${evt.metricName.digest},
            "serviceID": ${evt.serviceParams.serviceID}
          }
          """

  private def actionStart(evt: ActionStart): Json =
    json"""
          {       
            "event": "ActionStart",
            "name": ${evt.actionInfo.actionParams.metricName.origin},
            "digest": ${evt.actionInfo.actionParams.metricName.digest},
            "id": ${evt.actionID},
            "serviceID": ${evt.serviceParams.serviceID}
          }
          """

  private def actionRetrying(evt: ActionRetry): Json =
    json"""
          {       
            "event": "ActionRetry",
            "name": ${evt.actionInfo.actionParams.metricName.origin},
            "cause" : ${evt.error.message},
            "digest": ${evt.actionInfo.actionParams.metricName.digest},
            "id": ${evt.actionID},         
            "serviceID": ${evt.serviceParams.serviceID}
          }
          """

  private def actionFailed(evt: ActionFail): Json =
    json"""
          {       
            "event": "ActionFail",
            "name": ${evt.actionInfo.actionParams.metricName.origin},
            "notes" : ${evt.notes.value},
            "cause" : ${evt.error.stackTrace},
            "digest": ${evt.actionInfo.actionParams.metricName.digest},
            "id": ${evt.actionID},       
            "serviceID": ${evt.serviceParams.serviceID}
          }
          """

  private def actionSucced(evt: ActionSucc): Json =
    json"""
          {       
            "event": "ActionSucc",
            "name": ${evt.actionInfo.actionParams.metricName.origin},
            "notes" : ${evt.notes.value},
            "digest": ${evt.actionInfo.actionParams.metricName.digest},
            "id": ${evt.actionID},      
            "serviceID": ${evt.serviceParams.serviceID}
          }
          """

  def apply[F[_]: Applicative]: Translator[F, Json] =
    Translator
      .empty[F, Json]
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
