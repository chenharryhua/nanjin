package com.github.chenharryhua.nanjin.frontend

import io.circe.jawn.decode
import io.circe.syntax.EncoderOps

class FrontendDataTest extends munit.FunSuite {

  // --- Series codec ---

  test("1.Series codec round-trip") {
    val series = Series("cpu_usage", 75.5)
    val json = series.asJson.noSpaces
    val decoded = decode[Series](json)
    assertEquals(decoded, Right(series))
  }

  test("2.Series decodes from JSON string") {
    val json = """{"label":"memory","value":1024.0}"""
    val decoded = decode[Series](json)
    assertEquals(decoded, Right(Series("memory", 1024.0)))
  }

  // --- WsMessage codec ---

  test("3.WsMessage codec round-trip") {
    val msg = WsMessage(1700000000000.0, List(Series("a", 1.0), Series("b", 2.0)))
    val json = msg.asJson.noSpaces
    val decoded = decode[WsMessage](json)
    assertEquals(decoded, Right(msg))
  }

  test("4.WsMessage.points builds correct map") {
    val msg = WsMessage(123.0, List(Series("x", 10.0), Series("y", 20.0)))
    assertEquals(msg.points.size, 2)
    assertEquals(msg.points("x"), Point(123.0, Some(10.0)))
    assertEquals(msg.points("y"), Point(123.0, Some(20.0)))
  }

  test("5.WsMessage with empty series") {
    val msg = WsMessage(0.0, Nil)
    val json = msg.asJson.noSpaces
    val decoded = decode[WsMessage](json)
    assertEquals(decoded, Right(msg))
    assert(msg.points.isEmpty)
  }

  // --- Point ---

  test("6.Point with value") {
    val p = Point(100.0, Some(42.0))
    assertEquals(p.x, 100.0)
    assertEquals(p.y, Some(42.0))
  }

  test("7.Point without value (gap)") {
    val p = Point(200.0, None)
    assertEquals(p.x, 200.0)
    assertEquals(p.y, None)
  }
}
