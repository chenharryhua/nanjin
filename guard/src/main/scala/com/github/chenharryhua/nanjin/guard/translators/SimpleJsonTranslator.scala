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
            "service_name" : ${evt.serviceName},
            "cause" : ${evt.error.stackTrace},
            "serviceID": ${evt.serviceId},
            "timestamp": ${evt.timestamp}
          }
          """

  private def serviceStopped(evt: ServiceStop): Json =
    json"""
          {
            "event": "ServiceStop",
            "service_name" : ${evt.serviceName},
            "exitCode": ${evt.cause.exitCode},
            "cause": ${evt.cause.show},
            "serviceID": ${evt.serviceId},
            "timestamp": ${evt.timestamp}
          }
          """

  private def metricReport(evt: MetricReport): Json =
    json"""
          {
            "event": "MetricReport",
            "index": ${evt.reportType.idx},
            "service_name" : ${evt.serviceName},
            "is_up": ${evt.isUp},
            "ongoings": ${evt.ongoings.map(_.actionId)},
            "metrics": ${evt.snapshot.asJson},
            "service_id": ${evt.serviceId},
            "timestamp": ${evt.timestamp}
          }
          """

  private def metricReset(evt: MetricReset): Json =
    json"""
          {
            "event": "MetricReset",
            "service_name" : ${evt.serviceName},
            "metrics": ${evt.snapshot.asJson},
            "service_id": ${evt.serviceId},
            "timestamp": ${evt.timestamp}
          }
          """

  private def passThrough(evt: PassThrough): Json =
    json"""
          {
            "event": "PassThrough",
            "name": ${evt.digested.name},
            "is_error": ${evt.isError},
            "value": ${evt.value},
            "digest": ${evt.digested.digest},
            "service_id": ${evt.serviceId},
            "timestamp": ${evt.timestamp}
          }
          """

  private def instantAlert(evt: InstantAlert): Json =
    json"""
          {       
            "event": "InstantAlert",
            "name": ${evt.digested.name},
            "importance": ${evt.importance},
            "message": ${evt.message},
            "digest": ${evt.digested.digest},
            "service_id": ${evt.serviceId},
            "timestamp": ${evt.timestamp}
          }
          """

  private def actionStart(evt: ActionStart): Json =
    json"""
          {       
            "event": "ActionStart",
            "name": ${evt.digested.name},
            "trace_id": ${evt.traceId},
            "input": ${evt.input},
            "digest": ${evt.digested.digest},
            "id": ${evt.actionId},
            "service_id": ${evt.serviceId},
            "timestamp": ${evt.timestamp}
          }
          """

  private def actionRetrying(evt: ActionRetry): Json =
    json"""
          {       
            "event": "ActionRetry",
            "name": ${evt.digested.name},
            "trace_id": ${evt.traceId},
            "trace_uri": ${evt.traceUri},
            "cause" : ${evt.error.message},
            "digest": ${evt.digested.digest},
            "id": ${evt.actionId},
            "service_id": ${evt.serviceId},
            "timestamp": ${evt.timestamp}
          }
          """

  private def actionFailed(evt: ActionFail): Json =
    json"""
          {       
            "event": "ActionFail",
            "name": ${evt.digested.name},
            "traceID": ${evt.traceId},
            "traceUri": ${evt.traceUri},
            "input": ${evt.input},
            "cause" : ${evt.error.stackTrace},
            "digest": ${evt.digested.digest},
            "id": ${evt.actionId},
            "serviceID": ${evt.serviceId},
            "timestamp": ${evt.timestamp}
          }
          """

  private def actionSucced(evt: ActionSucc): Json =
    json"""
          {       
            "event": "ActionSucc",
            "name": ${evt.digested.name},
            "traceID": ${evt.traceId},
            "traceUri": ${evt.traceUri},
            "output": ${evt.output},
            "digest": ${evt.digested.digest},
            "id": ${evt.actionId},
            "serviceID": ${evt.serviceId},
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
