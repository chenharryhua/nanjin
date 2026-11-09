package mtest.common

import com.github.chenharryhua.nanjin.common.json
import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.FunSuite

import java.time.Duration
import java.util.concurrent.{Callable, Executors}

class JsonTest extends FunSuite {
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

  test("5.redact - only string values are redacted; container structure is preserved") {
    // a matching key whose value is an object keeps its structure (recursion still visits its leaves),
    // a matching key whose value is a number is left as-is, and matching string leaves are redacted.
    val in = Json.obj(
      "credentials" -> Json.obj("password" -> "p".asJson, "attempts" -> 3.asJson),
      "password" -> 1234.asJson,
      "keep" -> 42.asJson)
    val out = json.redact("credentials", "password")(in)
    // object-valued match is not collapsed into the marker; it stays an object
    assert(out.hcursor.downField("credentials").focus.exists(_.isObject))
    // the string leaf inside it is redacted by recursion
    assert(out.hcursor.downField("credentials").get[String]("password").toOption.contains(redacted))
    // its non-string sibling is untouched
    assert(out.hcursor.downField("credentials").get[Int]("attempts").toOption.contains(3))
    // a matching key holding a number is left unchanged (only strings are redacted)
    assert(out.hcursor.get[Int]("password").toOption.contains(1234))
    assert(out.hcursor.get[Int]("keep").toOption.contains(42))
  }

  test("6.redact - custom transform is applied to matched string values") {
    val in = Json.obj("card" -> "4111111111111111".asJson, "keep" -> "visible".asJson)
    val out = json.redact(List("card"), s => "*" * (s.length - 4) + s.takeRight(4))(in)
    assert(out.hcursor.get[String]("card").toOption.contains("*" * 12 + "1111"))
    assert(out.hcursor.get[String]("keep").toOption.contains("visible"))
  }

  test("7.redact - no keys is a no-op") {
    val in = Json.obj("password" -> "secret".asJson, "n" -> Json.arr(1.asJson, 2.asJson))
    assert(json.redact()(in) == in)
  }

  test("8.redact - no matching key leaves json unchanged") {
    val in = Json.obj("a" -> "1".asJson, "b" -> Json.obj("c" -> "2".asJson))
    assert(json.redact("zzz")(in) == in)
  }

  test("9.redact - non-object root is returned unchanged") {
    assert(json.redact("password")("hello".asJson) == "hello".asJson)
    assert(json.redact("password")(123.asJson) == 123.asJson)
  }

  test("10.redact - accepts a splatted collection") {
    val in = Json.obj("password" -> "p".asJson, "user" -> "alice".asJson)
    val configured = List("password")
    val out = json.redact(configured*)(in)
    assert(out.hcursor.get[String]("password").toOption.contains(redacted))
    assert(out.hcursor.get[String]("user").toOption.contains("alice"))
  }

  // ---------------- prettify ----------------

  test("11.prettify - formats a large number with grouping separators") {
    val in: Json = Json.obj("count" -> 1234567.asJson)
    val out = json.prettify(in)
    assert(out.hcursor.get[String]("count").toOption.contains("1,234,567"))
  }

  test("12.prettify - formats numbers nested in objects and arrays") {
    val in: Json =
      Json.obj("nested" -> Json.obj("big" -> 1000000.asJson), "list" -> Json.arr(2000.asJson, 3000.asJson))
    val out = json.prettify(in)
    assert(out.hcursor.downField("nested").get[String]("big").toOption.contains("1,000,000"))
    assert(out.hcursor.downField("list").downN(0).as[String].toOption.contains("2,000"))
    assert(out.hcursor.downField("list").downN(1).as[String].toOption.contains("3,000"))
  }

  test("13.prettify - reformats duration-encoded strings") {
    val in: Json = Json.obj("elapsed" -> Duration.ofSeconds(65).asJson)
    val out = json.prettify(in)
    assert(out.hcursor.get[String]("elapsed").toOption.contains("1 minute 5 seconds"))
  }

  test("14.prettify - leaves non-numeric, non-duration strings untouched") {
    val in: Json = Json.obj("name" -> "alice".asJson)
    val out = json.prettify(in)
    assert(out.hcursor.get[String]("name").toOption.contains("alice"))
  }

  test("15.prettify - preserves object keys") {
    val in: Json = Json.obj("a" -> 10.asJson, "b" -> 20.asJson)
    val out = json.prettify(in)
    assert(out.hcursor.keys.map(_.toSet).contains(Set("a", "b")))
  }

  test("16.prettify - keeps fractional digits instead of rounding to a whole number") {
    // the old grouping-only "#,###" pattern rounded these to "2" and "0"; the fraction must survive now
    val in: Json = Json.obj("rate" -> 1.5.asJson, "ratio" -> 0.25.asJson)
    val out = json.prettify(in)
    assert(out.hcursor.get[String]("rate").toOption.contains("1.5"))
    assert(out.hcursor.get[String]("ratio").toOption.contains("0.25"))
  }

  test("17.prettify - groups the integer part of a large fractional number") {
    val in: Json = Json.obj("amount" -> 1234567.89.asJson)
    val out = json.prettify(in)
    assert(out.hcursor.get[String]("amount").toOption.contains("1,234,567.89"))
  }

  test("21.prettify - reformats every string shape java.time.Duration accepts") {
    // Candidate strings are screened on an optional sign followed by P in either case, so the screen must
    // admit each of these. A duration left unformatted would show as its raw ISO text.
    val in: Json = Json.obj(
      "plain" -> "PT1M5S".asJson,
      "signed" -> "-PT1M5S".asJson,
      "plussed" -> "+PT1M5S".asJson,
      "lower" -> "pt1m5s".asJson,
      "days" -> "P1DT2H".asJson)
    val out = json.prettify(in)
    assert(out.hcursor.get[String]("plain").toOption.contains("1 minute 5 seconds"))
    assert(out.hcursor.get[String]("signed").toOption.contains("-1 minute 5 seconds"))
    assert(out.hcursor.get[String]("plussed").toOption.contains("1 minute 5 seconds"))
    assert(out.hcursor.get[String]("lower").toOption.contains("1 minute 5 seconds"))
    assert(out.hcursor.get[String]("days").toOption.contains("1 day 2 hours"))
  }

  test("22.prettify - leaves strings that only look like a duration untouched") {
    val in: Json =
      Json.obj("city" -> "Paris".asJson, "sign" -> "-".asJson, "word" -> "pending".asJson, "p" -> "P".asJson)
    val out = json.prettify(in)
    assert(out.hcursor.get[String]("city").toOption.contains("Paris"))
    assert(out.hcursor.get[String]("sign").toOption.contains("-"))
    assert(out.hcursor.get[String]("word").toOption.contains("pending"))
    assert(out.hcursor.get[String]("p").toOption.contains("P"))
  }

  test("18.prettify - does not lose precision for longs above 2^53") {
    // 9007199254740993 (2^53 + 1) is not representable as a double; it must not be rounded to ...992
    val in: Json = Json.obj("id" -> 9007199254740993L.asJson, "max" -> Long.MaxValue.asJson)
    val out = json.prettify(in)
    assert(out.hcursor.get[String]("id").toOption.contains("9,007,199,254,740,993"))
    assert(out.hcursor.get[String]("max").toOption.contains("9,223,372,036,854,775,807"))
  }

  test("19.prettify - formats negative and zero values") {
    val in: Json = Json.obj("neg" -> (-1234567).asJson, "zero" -> 0.asJson, "negFrac" -> (-0.5).asJson)
    val out = json.prettify(in)
    assert(out.hcursor.get[String]("neg").toOption.contains("-1,234,567"))
    assert(out.hcursor.get[String]("zero").toOption.contains("0"))
    assert(out.hcursor.get[String]("negFrac").toOption.contains("-0.5"))
  }

  test("20.prettify - is safe under concurrent use") {
    // DecimalFormat is not thread-safe; a shared instance corrupts output when used from many threads.
    val values = (0 until 8).map(i => (i + 1) * 1111111L + i)
    val expected = values.map(v => v -> json.prettify(v.asJson).asString.get).toMap
    val pool = Executors.newFixedThreadPool(16)
    try {
      val tasks = (0 until 64).map { _ =>
        pool.submit(new Callable[Int] {
          override def call(): Int =
            (0 until 500).map { _ =>
              values.count(v => json.prettify(v.asJson).asString.get != expected(v))
            }.sum
        })
      }
      assertEquals(tasks.map(_.get()).sum, 0)
    } finally pool.shutdown()
  }
}
