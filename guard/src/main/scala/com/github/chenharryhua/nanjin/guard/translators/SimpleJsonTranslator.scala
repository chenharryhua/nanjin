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
            "serviceID": ${evt.serviceParams.serviceID},
            "cause" : ${evt.error.stackTrace}
          }
          """

  private def serviceStopped(evt: ServiceStop): Json =
    json"""
          {
            "event": "ServiceStop",
            "serviceName" : ${evt.serviceParams.serviceName.value},
            "serviceID": ${evt.serviceParams.serviceID},
            "cause": ${evt.cause.show},
            "exitCode": ${evt.cause.exitCode}
          }
          """

  private def metricReport(evt: MetricReport): Json =
    json"""
          {
            "event": "MetricReport",
            "serviceName" : ${evt.serviceParams.serviceName.value},
            "serviceID": ${evt.serviceParams.serviceID},
            "ongoings": ${evt.ongoings.map(_.actionID)},
            "isUp": ${evt.isUp},
            "metrics": ${evt.snapshot.asJson}
          }
          """

  private def metricReset(evt: MetricReset): Json =
    json"""
          {
            "event": "MetricReset",
            "serviceName" : ${evt.serviceParams.serviceName.value},
            "serviceID": ${evt.serviceParams.serviceID},
            "metrics": ${evt.snapshot.asJson}
          }
          """

  private def passThrough(evt: PassThrough): Json =
    json"""
          {
            "event": "PassThrough",
            "name": ${evt.metricName.origin},
            "digest": ${evt.metricName.digest},
            "serviceID": ${evt.serviceParams.serviceID},
            "isError": ${evt.isError},
            "value": ${evt.value}
          }
          """

  private def instantAlert(evt: InstantAlert): Json =
    json"""
          {       
            "event": "InstantAlert",
            "name": ${evt.metricName.origin},
            "digest": ${evt.metricName.digest},
            "serviceID": ${evt.serviceParams.serviceID},
            "importance": ${evt.importance.value},
            "message": ${evt.message}    
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
            "digest": ${evt.actionInfo.actionParams.metricName.digest},
            "id": ${evt.actionID},         
            "serviceID": ${evt.serviceParams.serviceID},
            "cause" : ${evt.error.message}
          }
          """

  private def actionFailed(evt: ActionFail): Json =
    json"""
          {       
            "event": "ActionFail",
            "name": ${evt.actionInfo.actionParams.metricName.origin},
            "digest": ${evt.actionInfo.actionParams.metricName.digest},
            "id": ${evt.actionID},       
            "serviceID": ${evt.serviceParams.serviceID},
            "cause" : ${evt.error.stackTrace},
            "notes" : ${evt.notes.value}
          }
          """

  private def actionSucced(evt: ActionSucc): Json =
    json"""
          {       
            "event": "ActionSucc",
            "name": ${evt.actionInfo.actionParams.metricName.origin},
            "digest": ${evt.actionInfo.actionParams.metricName.digest},
            "id": ${evt.actionID},      
            "serviceID": ${evt.serviceParams.serviceID},
            "notes" : ${evt.notes.value}
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
