package com.github.chenharryhua.nanjin.guard.service.dashboard

import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.{MetricElement, Snapshot}
import com.github.chenharryhua.nanjin.guard.metrics.MetricId
import io.circe.Json
import mtest.guard.gendata.{metricsSnapshot, servicePanic}
import munit.FunSuite

import java.time.Instant

class DashboardDocumentsTest extends FunSuite {

  private def health(json: Json): Option[Boolean] =
    json.hcursor.downField("healthy").as[Boolean].toOption

  private def health_metric_id: MetricId =
    Json
      .obj(
        "scope" -> Json.obj(
          "label" -> Json.fromString("dependency"),
          "domain" -> Json.fromString("test"),
          "service" -> Json.fromString("dashboard"),
          "task" -> Json.fromString("health")),
        "token" -> Json.obj(
          "metricName" -> Json.fromString("health"),
          "age" -> Json.fromLong(1L),
          "uniqueToken" -> Json.fromInt(1)),
        "category" -> Json.obj(
          "Gauge" -> Json.obj(
            "kind" -> Json.obj("HealthCheck" -> Json.obj()),
            "isCached" -> Json.fromBoolean(false)))
      )
      .as[MetricId]
      .fold(error => fail(s"invalid health MetricId fixture: $error"), identity)

  private def health_snapshot(value: Boolean): MetricsSnapshot = {
    val gauge = MetricElement.Gauge(health_metric_id, MetricElement.GaugeData(Json.fromBoolean(value)))
    metricsSnapshot.copy(snapshot = Snapshot(Nil, Nil, Nil, Nil, List(gauge)))
  }

  test("1.health check is healthy with no panic and no dependency snapshots") {
    val result = documents.service_health_check(Vector.empty, Vector.empty, Instant.EPOCH)
    assertEquals(result.flatMap(json => health(json).toRight("missing healthy field")), Right(true))
  }

  test("2.an active panic window returns a restart message") {
    val now = servicePanic.tick.conclude.minusSeconds(1L)
    val result = documents.service_health_check(Vector(servicePanic), Vector.empty, now)

    result match {
      case Left(message) =>
        assert(message.contains("Service panic detected"), message)
        assert(message.contains("Restarting in"), message)
      case Right(json) => fail(s"expected unavailable health, got $json")
    }
  }

  test("3.an expired panic window returns dependency health") {
    val now = servicePanic.tick.conclude.plusSeconds(1L)
    val result = documents.service_health_check(Vector(servicePanic), Vector.empty, now)
    assertEquals(result.flatMap(json => health(json).toRight("missing healthy field")), Right(true))
  }

  test("4.latest dependency health checks determine healthy status") {
    val old_unhealthy = health_snapshot(false)
    val latest_healthy = health_snapshot(true)
    val healthy_result =
      documents.service_health_check(Vector.empty, Vector(old_unhealthy, latest_healthy), Instant.EPOCH)
    val unhealthy_result =
      documents.service_health_check(Vector.empty, Vector(latest_healthy, old_unhealthy), Instant.EPOCH)

    assertEquals(healthy_result.flatMap(json => health(json).toRight("missing healthy field")), Right(true))
    assertEquals(
      unhealthy_result.flatMap(json => health(json).toRight("missing healthy field")),
      Right(false))
  }

  test("5.mxBeans returns populated runtime, memory, thread, classloader, GC, and OS data") {
    val jvm = mxBeans.allJvmGauge.value

    assert(jvm.runtime.pid > 0L)
    assert(jvm.runtime.virtual_machine.nonEmpty)
    assert(jvm.operating_system.architecture.nonEmpty)
    assert(jvm.operating_system.available_processors > 0)
    assert(jvm.classloader.loaded > 0)
    assert(jvm.classloader.total >= jvm.classloader.loaded)
    assert(jvm.heap_memory.used >= 0L)
    assert(jvm.non_heap_memory.used >= 0L)
    assert(jvm.thread_state.live > 0)
    assert(jvm.thread_state.peak >= jvm.thread_state.live)
    assert(jvm.garbage_collectors.forall(_.name.nonEmpty))
    assert(jvm.deadlocks.forall(deadlock => deadlock.thread.nonEmpty && deadlock.stack.nonEmpty))
  }
}
