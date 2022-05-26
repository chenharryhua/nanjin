package com.github.chenharryhua.nanjin.guard.translators

import cats.Applicative
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.circe.syntax.*

private object SimpleJsonTranslator {
  import NJEvent.*

  private def serviceStarted(evt: ServiceStart): Json =
    json"""
          { 
            "event": "ServiceStart",
            "params": ${evt.serviceParams.asJson}, 
            "timestamp": ${evt.timestamp}
          }
          """

  private def servicePanic(evt: ServicePanic): Json =
    json"""
          {
            "event": "ServicePanic",
            "serviceName" : ${evt.serviceParams.serviceName.value},
            "cause" : ${evt.error.stackTrace},
            "serviceID": ${evt.serviceParams.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  private def serviceStopped(evt: ServiceStop): Json =
    json"""
          {
            "event": "ServiceStop",
            "serviceName" : ${evt.serviceParams.serviceName.value},
            "exitCode": ${evt.cause.exitCode},
            "cause": ${evt.cause.show},
            "serviceID": ${evt.serviceParams.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  private def metricReport(evt: MetricReport): Json =
    json"""
          {
            "event": "MetricReport",
            "index": ${evt.reportType.idx},
            "serviceName" : ${evt.serviceParams.serviceName.value},
            "isUp": ${evt.isUp},
            "ongoings": ${evt.ongoings.map(_.actionID)},            
            "metrics": ${evt.snapshot.asJson},
            "serviceID": ${evt.serviceParams.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  private def metricReset(evt: MetricReset): Json =
    json"""
          {
            "event": "MetricReset",
            "serviceName" : ${evt.serviceParams.serviceName.value},
            "metrics": ${evt.snapshot.asJson},
            "serviceID": ${evt.serviceParams.serviceID},
            "timestamp": ${evt.timestamp}
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
            "serviceID": ${evt.serviceParams.serviceID},
            "timestamp": ${evt.timestamp}
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
            "serviceID": ${evt.serviceParams.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  private def actionStart(evt: ActionStart): Json =
    json"""
          {       
            "event": "ActionStart",
            "name": ${evt.actionInfo.actionParams.metricName.origin},
            "info": ${evt.info},
            "digest": ${evt.actionInfo.actionParams.metricName.digest},
            "id": ${evt.actionID},
            "serviceID": ${evt.serviceParams.serviceID},
            "timestamp": ${evt.timestamp}
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
            "serviceID": ${evt.serviceParams.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  private def actionFailed(evt: ActionFail): Json =
    json"""
          {       
            "event": "ActionFail",
            "name": ${evt.actionInfo.actionParams.metricName.origin},
            "notes" : ${evt.notes.map(_.value)},
            "cause" : ${evt.error.stackTrace},
            "digest": ${evt.actionInfo.actionParams.metricName.digest},
            "id": ${evt.actionID},       
            "serviceID": ${evt.serviceParams.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  private def actionSucced(evt: ActionSucc): Json =
    json"""
          {       
            "event": "ActionSucc",
            "name": ${evt.actionInfo.actionParams.metricName.origin},
            "notes" : ${evt.notes.map(_.value)},
            "digest": ${evt.actionInfo.actionParams.metricName.digest},
            "id": ${evt.actionID},      
            "serviceID": ${evt.serviceParams.serviceID},
            "timestamp": ${evt.timestamp}
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
