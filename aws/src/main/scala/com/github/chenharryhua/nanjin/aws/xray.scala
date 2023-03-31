package com.github.chenharryhua.nanjin.aws

import org.typelevel.ci.{CIString, CIStringSyntax}

import java.time.Instant
import java.util.UUID

object xray {
  final val headerName: CIString = ci"X-Amzn-Trace-Id"

  // build trace-id from time and uuid
  def traceId(time: Instant, uuid: UUID): String =
    s"1-${time.getEpochSecond.toHexString}-${uuid.toString.takeRight(12)}"
}
