package com.github.chenharryhua.nanjin.guard

import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import io.circe.Json
import io.circe.syntax.EncoderOps

package object config {
  private[config] def interpretServiceParams(serviceParams: ServiceParams): Json =
    Json.obj(
      Attribute(serviceParams.taskName).snakeJsonEntry,
      Attribute(serviceParams.serviceName).snakeJsonEntry,
      Attribute(serviceParams.serviceId).snakeJsonEntry,
      "homepage" -> serviceParams.homepage.asJson,
      Attribute(serviceParams.host).snakeJsonEntry,
      "service_policies" -> Json.obj(
        "restart" -> Json.obj(
          Attribute(serviceParams.servicePolicies.restart.policy).snakeJsonEntry(_.show.asJson),
          "threshold" -> serviceParams.servicePolicies.restart.threshold.map(defaultFormatter.format).asJson
        ),
        "metrics_report" -> serviceParams.servicePolicies.metricsReport.show.asJson,
        "metrics_reset" -> serviceParams.servicePolicies.metricsReset.show.asJson
      ),
      "launch_time" -> serviceParams.launchTime.asJson,
      "log_format" -> serviceParams.logFormat.asJson,
      "history_capacity" -> Json.obj(
        "metrics_queue_size" -> serviceParams.historyCapacity.metric.asJson,
        "error_queue_size" -> serviceParams.historyCapacity.error.asJson,
        "panic_queue_size" -> serviceParams.historyCapacity.panic.asJson
      ),
      "nanjin" -> serviceParams.nanjin.asJson,
      Attribute(serviceParams.brief).snakeJsonEntry
    )
}
