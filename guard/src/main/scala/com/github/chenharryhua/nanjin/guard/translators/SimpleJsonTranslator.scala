package com.github.chenharryhua.nanjin.guard.translators

import cats.Applicative
import cats.syntax.show.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.circe.refined.*
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
            "serviceName" : ${evt.serviceName},
            "cause" : ${evt.error.stackTrace},
            "serviceID": ${evt.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  private def serviceStopped(evt: ServiceStop): Json =
    json"""
          {
            "event": "ServiceStop",
            "serviceName" : ${evt.serviceName},
            "exitCode": ${evt.cause.exitCode},
            "cause": ${evt.cause.show},
            "serviceID": ${evt.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  private def metricReport(evt: MetricReport): Json =
    json"""
          {
            "event": "MetricReport",
            "index": ${evt.reportType.idx},
            "serviceName" : ${evt.serviceName},
            "isUp": ${evt.isUp},
            "ongoings": ${evt.ongoings.map(_.actionID)},            
            "metrics": ${evt.snapshot.asJson},
            "serviceID": ${evt.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  private def metricReset(evt: MetricReset): Json =
    json"""
          {
            "event": "MetricReset",
            "serviceName" : ${evt.serviceName},
            "metrics": ${evt.snapshot.asJson},
            "serviceID": ${evt.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  private def passThrough(evt: PassThrough): Json =
    json"""
          {
            "event": "PassThrough",
            "name": ${evt.name.origin},
            "isError": ${evt.isError},
            "value": ${evt.value},
            "digest": ${evt.name.digest},
            "serviceID": ${evt.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  private def instantAlert(evt: InstantAlert): Json =
    json"""
          {       
            "event": "InstantAlert",
            "name": ${evt.name.origin},
            "importance": ${evt.importance},
            "message": ${evt.message},
            "digest": ${evt.name.digest},
            "serviceID": ${evt.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  private def actionStart(evt: ActionStart): Json =
    json"""
          {       
            "event": "ActionStart",
            "name": ${evt.actionInfo.actionParams.name.origin},
            "input": ${evt.input},
            "digest": ${evt.actionInfo.actionParams.name.digest},
            "id": ${evt.actionID},
            "serviceID": ${evt.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  private def actionRetrying(evt: ActionRetry): Json =
    json"""
          {       
            "event": "ActionRetry",
            "name": ${evt.actionInfo.actionParams.name.origin},
            "cause" : ${evt.error.message},
            "digest": ${evt.actionInfo.actionParams.name.digest},
            "id": ${evt.actionID},         
            "serviceID": ${evt.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  private def actionFailed(evt: ActionFail): Json =
    json"""
          {       
            "event": "ActionFail",
            "name": ${evt.actionInfo.actionParams.name.origin},
            "input": ${evt.input},
            "cause" : ${evt.error.stackTrace},
            "digest": ${evt.actionInfo.actionParams.name.digest},
            "id": ${evt.actionID},       
            "serviceID": ${evt.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  private def actionSucced(evt: ActionSucc): Json =
    json"""
          {       
            "event": "ActionSucc",
            "name": ${evt.actionInfo.actionParams.name.origin},
            "output": ${evt.output},
            "digest": ${evt.actionInfo.actionParams.name.digest},
            "id": ${evt.actionID},      
            "serviceID": ${evt.serviceID},
            "timestamp": ${evt.timestamp}
          }
          """

  def apply[F[_]: Applicative]: Translator[F, Json] =
    Translator
      .empty[F, Json]
      .withServiceStart(serviceStarted)
      .withServiceStop(serviceStopped)
      .withServicePanic(servicePanic)
      .withMetricReport(metricReport)
      .withMetricReset(metricReset)
      .withPassThrough(passThrough)
      .withInstantAlert(instantAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying)
      .withActionFail(actionFailed)
      .withActionSucc(actionSucced)
}
