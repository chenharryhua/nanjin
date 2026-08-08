package mtest.common

import com.github.chenharryhua.nanjin.common.xml2Json
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import scala.xml.{Comment, Text, XML}

class Xml2JsonTest extends AnyFunSuite {

  test("1.simple text node") {
    val xml = XML.loadString("<name>alice</name>")

    val expected = Json.obj("name" -> Json.fromString("alice"))

    assert(xml2Json(xml) == expected)
  }

  test("2.attributes with text") {
    val xml = XML.loadString("<user id=\"42\" active=\"yes\">tom</user>")

    val expected = Json.obj(
      "user" -> Json.obj(
        "@id" -> Json.fromString("42"),
        "@active" -> Json.fromString("yes"),
        "#text" -> Json.fromString("tom")
      ))

    assert(xml2Json(xml) == expected)
  }

  test("3.repeated child elements become array") {
    val xml = XML.loadString("""
                               |<users>
                               |  <user>alice</user>
                               |  <user>bob</user>
                               |</users>
                               |""".stripMargin)

    val expected = Json.obj(
      "users" -> Json.obj(
        "user" -> List("alice", "bob").asJson
      ))

    assert(xml2Json(xml) == expected)
  }

  test("4.mixed content keeps text and children") {
    val xml = XML.loadString("""
                               |<root kind="k">
                               |  hello
                               |  <child>one</child>
                               |  world
                               |</root>
                               |""".stripMargin)

    val expected = Json.obj(
      "root" -> Json.obj(
        "@kind" -> Json.fromString("k"),
        "#text" -> Json.fromString("helloworld"),
        "child" -> Json.fromString("one"),
        "#children" -> List(
          Json.obj("#text" -> Json.fromString("hello")),
          Json.obj("child" -> Json.fromString("one")),
          Json.obj("#text" -> Json.fromString("world"))
        ).asJson
      ))

    assert(xml2Json(xml) == expected)
  }

  test("5.attribute-only element includes empty #text") {
    val xml = XML.loadString("<user id=\"42\"/>")

    val expected = Json.obj(
      "user" -> Json.obj(
        "@id" -> Json.fromString("42"),
        "#text" -> Json.fromString("")
      ))

    assert(xml2Json(xml) == expected)
  }

  test("6.top-level unsupported node throws") {
    assertThrows[RuntimeException](xml2Json(Comment("not-an-elem")))
  }

  test("7.mixed content preserves ordering in #children") {
    val xml = XML.loadString("<root>pre<x>1</x>mid<y>2</y>post</root>")

    val expected = Json.obj(
      "root" -> Json.obj(
        "#text" -> Json.fromString("premidpost"),
        "x" -> Json.fromString("1"),
        "y" -> Json.fromString("2"),
        "#children" -> List(
          Json.obj("#text" -> Json.fromString("pre")),
          Json.obj("x" -> Json.fromString("1")),
          Json.obj("#text" -> Json.fromString("mid")),
          Json.obj("y" -> Json.fromString("2")),
          Json.obj("#text" -> Json.fromString("post"))
        ).asJson
      ))

    assert(xml2Json(xml) == expected)
  }

  test("8.root text preserves spaces but nested text is trimmed") {
    val rootText = xml2Json(Text("  hi  "))
    val nestedText = xml2Json(XML.loadString("<a>  hi  </a>"))

    assert(rootText == Json.fromString("  hi  "))
    assert(nestedText == Json.obj("a" -> Json.fromString("hi")))
  }
}
