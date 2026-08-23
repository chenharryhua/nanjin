package com.github.chenharryhua.nanjin.guard.observers

import com.github.chenharryhua.nanjin.guard.config.{Brief, CloudWatchLogs}
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant

class CloudWatchLogsTest extends AnyFunSuite {
  private val logOptions = Json.obj(
    "awslogs-group" -> Json.fromString("/ecs/my-service"),
    "awslogs-region" -> Json.fromString("ap-southeast-2"),
    "awslogs-stream" -> Json.fromString("ecs/kafka_spark/051cbde2-2ac1-4b26-b6a5-30a3c07b0adc")
  )

  private val ts = Instant.ofEpochMilli(1700000000000L) // 2023-11-14T22:13:20Z

  test("logLink encodes the log group, stream, and time window") {
    val brief = Brief(Json.arr(Json.obj("LogOptions" -> logOptions)))

    val link = CloudWatchLogs.logLink(brief, ts)

    assert(link.isDefined)
    val url = link.get
    // log group and stream encoded
    assert(url.contains("$252Fecs$252Fmy-service"))
    assert(url.contains("ecs$252Fkafka_spark$252F051cbde2-2ac1-4b26-b6a5-30a3c07b0adc"))
    // time window: ±30s
    assert(url.contains(s"$$3Fstart$$3D${ts.minusSeconds(30).toEpochMilli}"))
    assert(url.contains(s"$$26end$$3D${ts.plusSeconds(30).toEpochMilli}"))
    // no filterPattern
    assert(!url.contains("filterPattern"))
  }

  test("logLink supports a brief containing a single LogOptions object") {
    val brief = Brief(Json.obj("LogOptions" -> logOptions))

    assert(CloudWatchLogs.logLink(brief, ts).nonEmpty)
  }

  test("logLink returns none when LogOptions is absent") {
    assert(CloudWatchLogs.logLink(Brief(Json.obj("other" -> Json.Null)), ts).isEmpty)
  }

  test("logLink returns none when awslogs-stream is missing") {
    val incomplete = Json.obj(
      "awslogs-group" -> Json.fromString("/ecs/my-service"),
      "awslogs-region" -> Json.fromString("ap-southeast-2")
    )
    assert(CloudWatchLogs.logLink(Brief(Json.obj("LogOptions" -> incomplete)), ts).isEmpty)
  }
}
