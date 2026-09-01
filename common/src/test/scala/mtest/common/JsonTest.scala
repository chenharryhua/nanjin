package mtest.common

import com.github.chenharryhua.nanjin.common.json
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration

class JsonTest extends AnyFunSuite {
  private val redacted = "redacted(*****)"

  // ---------------- redact ----------------

  test("1.redact - replaces a matching top-level key") {
    val in = Json.obj("password" -> "secret".asJson, "user" -> "alice".asJson)
    val out = json.redact("password")(in)
    assert(out.hcursor.get[String]("password").toOption.contains(redacted))
    assert(out.hcursor.get[String]("user").toOption.contains("alice"))
  }

  test("2.redact - replaces matching keys nested inside objects") {
    val in = Json.obj("outer" -> Json.obj("password" -> "p".asJson, "ok" -> 1.asJson))
    val out = json.redact("password")(in)
    val outer = out.hcursor.downField("outer")
    assert(outer.get[String]("password").toOption.contains(redacted))
    assert(outer.get[Int]("ok").toOption.contains(1))
  }

  test("3.redact - replaces matching keys inside array elements") {
    val in =
      Json.obj("items" -> Json.arr(Json.obj("password" -> "x".asJson), Json.obj("password" -> "y".asJson)))
    val out = json.redact("password")(in)
    val arr = out.hcursor.downField("items")
    assert(arr.downN(0).get[String]("password").toOption.contains(redacted))
    assert(arr.downN(1).get[String]("password").toOption.contains(redacted))
  }

  test("4.redact - redacts every key given") {
    val in = Json.obj("a" -> "1".asJson, "b" -> "2".asJson, "c" -> "3".asJson)
    val out = json.redact("a", "c")(in)
    assert(out.hcursor.get[String]("a").toOption.contains(redacted))
    assert(out.hcursor.get[String]("b").toOption.contains("2"))
    assert(out.hcursor.get[String]("c").toOption.contains(redacted))
  }

  test("5.redact - redacts a matching key regardless of value type") {
    val in = Json.obj("secret" -> Json.obj("nested" -> "deep".asJson), "keep" -> 42.asJson)
    val out = json.redact("secret")(in)
    assert(out.hcursor.get[String]("secret").toOption.contains(redacted))
    assert(out.hcursor.get[Int]("keep").toOption.contains(42))
  }

  test("6.redact - no keys is a no-op") {
    val in = Json.obj("password" -> "secret".asJson, "n" -> Json.arr(1.asJson, 2.asJson))
    assert(json.redact()(in) == in)
  }

  test("7.redact - no matching key leaves json unchanged") {
    val in = Json.obj("a" -> "1".asJson, "b" -> Json.obj("c" -> "2".asJson))
    assert(json.redact("zzz")(in) == in)
  }

  test("8.redact - non-object root is returned unchanged") {
    assert(json.redact("password")("hello".asJson) == "hello".asJson)
    assert(json.redact("password")(123.asJson) == 123.asJson)
  }

  test("8a.redact - accepts a splatted collection") {
    val in = Json.obj("password" -> "p".asJson, "user" -> "alice".asJson)
    val configured = List("password")
    val out = json.redact(configured*)(in)
    assert(out.hcursor.get[String]("password").toOption.contains(redacted))
    assert(out.hcursor.get[String]("user").toOption.contains("alice"))
  }

  // ---------------- prettify ----------------

  test("9.prettify - formats a large number with grouping separators") {
    val in: Json = Json.obj("count" -> 1234567.asJson)
    val out = json.prettify(in)
    assert(out.hcursor.get[String]("count").toOption.contains("1,234,567"))
  }

  test("10.prettify - formats numbers nested in objects and arrays") {
    val in: Json =
      Json.obj("nested" -> Json.obj("big" -> 1000000.asJson), "list" -> Json.arr(2000.asJson, 3000.asJson))
    val out = json.prettify(in)
    assert(out.hcursor.downField("nested").get[String]("big").toOption.contains("1,000,000"))
    assert(out.hcursor.downField("list").downN(0).as[String].toOption.contains("2,000"))
    assert(out.hcursor.downField("list").downN(1).as[String].toOption.contains("3,000"))
  }

  test("11.prettify - reformats duration-encoded strings") {
    val in: Json = Json.obj("elapsed" -> Duration.ofSeconds(65).asJson)
    val out = json.prettify(in)
    assert(out.hcursor.get[String]("elapsed").toOption.contains("1 minute 5 seconds"))
  }

  test("12.prettify - leaves non-numeric, non-duration strings untouched") {
    val in: Json = Json.obj("name" -> "alice".asJson)
    val out = json.prettify(in)
    assert(out.hcursor.get[String]("name").toOption.contains("alice"))
  }

  test("13.prettify - preserves object keys") {
    val in: Json = Json.obj("a" -> 10.asJson, "b" -> 20.asJson)
    val out = json.prettify(in)
    assert(out.hcursor.keys.map(_.toSet).contains(Set("a", "b")))
  }
}
