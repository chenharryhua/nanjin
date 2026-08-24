package com.github.chenharryhua.nanjin.guard.service.dashboard

import cats.syntax.show.given
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.translator.Attribute
import io.circe.Json
import io.circe.syntax.given

private def interpretServiceParams(serviceParams: ServiceParams): Json =
  Json.obj(
    Attribute(serviceParams.serviceIdentity.task).snakeJsonEntry,
    Attribute(serviceParams.serviceIdentity.service).snakeJsonEntry,
    Attribute(serviceParams.serviceIdentity.serviceId).snakeJsonEntry,
    Attribute(serviceParams.serviceIdentity.homepage).snakeJsonEntry,
    Attribute(serviceParams.serviceIdentity.host).map(_.show).snakeJsonEntry,
    "service_policies" -> Json.obj(
      "restart" -> Json.obj(
        Attribute(serviceParams.policies.restart.policy).map(_.show).snakeJsonEntry,
        "threshold" -> serviceParams.policies.restart.threshold.map(defaultFormatter.format).asJson
      ),
      "dashboard" ->
        serviceParams.policies.dashboard.map { tm =>
          Json.obj(
            Attribute(tm.policy).map(_.show).snakeJsonEntry,
            Attribute(tm.maxPoints).snakeJsonEntry
          )
        }.asJson,
      "metrics_report" -> serviceParams.policies.report.show.asJson
    ),
    Attribute(serviceParams.logFormat).snakeJsonEntry,
    "history_capacity" -> serviceParams.history.asJson,
    Attribute(serviceParams.serviceIdentity.launchTime).map(_.show).snakeJsonEntry,
    Attribute(serviceParams.serviceIdentity.timeZone).snakeJsonEntry,
    "nanjin" -> serviceParams.nanjin.asJson,
    Attribute(serviceParams.brief).snakeJsonEntry
  )
