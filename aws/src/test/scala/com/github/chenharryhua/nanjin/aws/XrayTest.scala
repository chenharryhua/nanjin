package com.github.chenharryhua.nanjin.aws

import org.scalatest.funsuite.AnyFunSuite

import java.time.{ZoneId, ZonedDateTime}
import java.util.UUID

class XrayTest extends AnyFunSuite {
  test("trace id") {
    val uuid = UUID.fromString("fc916b60-ee0e-4e31-8c83-7b74f6bd49b9")
    val time = ZonedDateTime.of(2023, 3, 20, 10, 30, 30, 0, ZoneId.of("Australia/Sydney"))
    assert(xray.traceId(time.toInstant, uuid) == "1-64179b16-7b74f6bd49b9")
  }
}
