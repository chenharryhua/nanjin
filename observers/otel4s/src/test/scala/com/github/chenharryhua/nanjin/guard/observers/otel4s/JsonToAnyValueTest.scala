package com.github.chenharryhua.nanjin.guard.observers.otel4s

import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.FunSuite
import org.typelevel.otel4s.AnyValue

/** Unit tests for the `JsonToAnyValue` conversion: each circe `Json` shape must map to the corresponding
  * otel4s `AnyValue`, with numbers kept as `long` when integral and `double` otherwise, and nesting
  * preserved.
  */
class JsonToAnyValueTest extends FunSuite {

  test("1.Json.Null becomes AnyValue.empty") {
    assertEquals(JsonToAnyValue(Json.Null), AnyValue.empty)
  }

  test("2.boolean becomes AnyValue.boolean") {
    assertEquals(JsonToAnyValue(Json.True), AnyValue.boolean(true))
    assertEquals(JsonToAnyValue(Json.False), AnyValue.boolean(false))
  }

  test("3.string becomes AnyValue.string") {
    assertEquals(JsonToAnyValue("hello".asJson), AnyValue.string("hello"))
  }

  test("4.an integral number becomes AnyValue.long") {
    assertEquals(JsonToAnyValue(42.asJson), AnyValue.long(42L))
    assertEquals(JsonToAnyValue(Long.MaxValue.asJson), AnyValue.long(Long.MaxValue))
    assertEquals(JsonToAnyValue(0.asJson), AnyValue.long(0L))
  }

  test("5.a fractional number becomes AnyValue.double") {
    assertEquals(JsonToAnyValue(3.14.asJson), AnyValue.double(3.14))
  }

  test("6.a number beyond Long range falls back to AnyValue.double") {
    val tooBig = Json.fromBigDecimal(BigDecimal(Long.MaxValue) + 1)
    assertEquals(JsonToAnyValue(tooBig), AnyValue.double((BigDecimal(Long.MaxValue) + 1).toDouble))
  }

  test("7.an array becomes AnyValue.seq preserving order") {
    val json = Json.arr(1.asJson, "a".asJson, Json.True)
    assertEquals(
      JsonToAnyValue(json),
      AnyValue.seq(List(AnyValue.long(1L), AnyValue.string("a"), AnyValue.boolean(true))))
  }

  test("8.an object becomes AnyValue.map") {
    val json = Json.obj("n" -> 1.asJson, "s" -> "x".asJson)
    assertEquals(
      JsonToAnyValue(json),
      AnyValue.map(Map("n" -> AnyValue.long(1L), "s" -> AnyValue.string("x"))))
  }

  test("9.nested objects and arrays convert recursively") {
    val json = Json.obj(
      "outer" -> Json.obj(
        "list" -> Json.arr(Json.obj("k" -> 2.asJson), Json.Null)
      ))
    val expected = AnyValue.map(
      Map("outer" -> AnyValue.map(
        Map("list" -> AnyValue.seq(List(AnyValue.map(Map("k" -> AnyValue.long(2L))), AnyValue.empty))))))
    assertEquals(JsonToAnyValue(json), expected)
  }
}
