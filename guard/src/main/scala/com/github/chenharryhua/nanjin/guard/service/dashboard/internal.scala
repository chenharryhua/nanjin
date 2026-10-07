package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.syntax.show.given
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.translator.Labelled
import io.circe.Json
import io.circe.syntax.given

private def interpretServiceParams(serviceParams: ServiceParams, logThreshold: Json): Json =
  Json.obj(
    Labelled(serviceParams.serviceIdentity.task).snakeJsonEntry,
    Labelled(serviceParams.serviceIdentity.service).snakeJsonEntry,
    Labelled(serviceParams.serviceIdentity.serviceId).snakeJsonEntry,
    Labelled(serviceParams.serviceIdentity.homepage).snakeJsonEntry,
    Labelled(serviceParams.serviceIdentity.host).map(_.show).snakeJsonEntry,
    "service_policies" -> Json.obj(
      "restart" -> Json.obj(
        Labelled(serviceParams.policies.restart.policy).map(_.show).snakeJsonEntry,
        "threshold" -> serviceParams.policies.restart.threshold.map(defaultFormatter.format).asJson
      ),
      "dashboard" ->
        serviceParams.policies.dashboard.map { tm =>
          Json.obj(
            Labelled(tm.policy).map(_.show).snakeJsonEntry,
            Labelled(tm.maxPoints).snakeJsonEntry
          )
        }.asJson,
      "metrics_report" -> serviceParams.policies.report.show.asJson
    ),
    Labelled(serviceParams.logFormat).snakeJsonEntry,
    "log_threshold" -> logThreshold,
    "history_capacity" -> serviceParams.history.asJson,
    Labelled(serviceParams.serviceIdentity.launchTime).map(_.show).snakeJsonEntry,
    Labelled(serviceParams.serviceIdentity.timeZone).snakeJsonEntry,
    "nanjin" -> serviceParams.nanjin.asJson,
    Labelled(serviceParams.brief).snakeJsonEntry
  )
