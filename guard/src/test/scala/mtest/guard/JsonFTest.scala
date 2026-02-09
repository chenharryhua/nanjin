package mtest.guard

import com.github.chenharryhua.nanjin.guard.translator.JsonF
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

class JsonFTest extends AnyFunSuite {
  test("json obj") {
    val json =
      Json.obj(
        "str" -> Json.fromString("str"),
        "bool" -> Json.fromBoolean(true),
        "num" -> Json.fromLong(10),
        "obj" -> Json.obj("a" -> 1.asJson, "b" -> 2.asJson, "c" -> 3.asJson),
        "arrInt" -> List(1, 2, 3).asJson,
        "arrStr" -> List("a", "b", "c").asJson,
        "nullType" -> Json.Null
      )
    JsonF.yml("name", json).foreach(println)
  }

  test("string") {
    val json = "string".asJson
    assert(JsonF.yml("name", json).head == "name: string")
  }

  test("number") {
    val json = Json.fromLong(1)
    assert(JsonF.yml("name", json).head == "name: 1")
  }

  test("boolean") {
    val json = Json.fromBoolean(true)
    assert(JsonF.yml("name", json).head == "name: true")
  }

  test("array") {
    val json = List(true, true, false).asJson
    assert(JsonF.yml("name", json).head == "name: [true, true, false]")
  }

  test("array - json") {
    val json = List(true.asJson, Json.Null, false.asJson).asJson
    assert(JsonF.yml("name", json).head == "name: [true, null, false]")
  }

  test("two layers") {
    val json = Json.obj(
      "top" ->
        Json.obj(
          "str" -> Json.fromString("str"),
          "arrStr" -> List("a", "b", "c").asJson,
          "nullType" -> Json.Null
        ))
    println(JsonF.yml("name", json))
  }
}
