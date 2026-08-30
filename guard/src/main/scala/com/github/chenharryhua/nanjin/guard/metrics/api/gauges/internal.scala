package com.github.chenharryhua.nanjin.guard.metrics.api.gauges

import com.github.chenharryhua.nanjin.guard.config.StackTrace
import io.circe.Json
import io.circe.syntax.EncoderOps

private def translateError(ex: Throwable): Json = StackTrace(ex).headOption.asJson
