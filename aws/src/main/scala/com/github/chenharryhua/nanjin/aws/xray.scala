package com.github.chenharryhua.nanjin.aws

import org.typelevel.ci.{CIString, CIStringSyntax}

import java.time.Instant
import java.util.UUID

object xray {
  final val headerName: CIString = ci"X-Amzn-Trace-Id"

  // build trace-id from time and uuid
  def traceId(time: Instant, uuid: UUID): String = {
    val timePart = String.format("%08x", time.getEpochSecond)
    val uuidHex = uuid.toString.replace("-", "")
    val uniquePart = uuidHex.take(24).padTo(24, '0')
    s"1-$timePart-$uniquePart"
  }
}
