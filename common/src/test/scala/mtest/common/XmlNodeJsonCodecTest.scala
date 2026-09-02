package mtest.common

import com.github.chenharryhua.nanjin.common.xml.given
import io.circe.{Codec, Json}
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import scala.xml.{Comment, Node, Text, XML}

class XmlNodeJsonCodecTest extends AnyFunSuite {

  private val codec: Codec[Node] = summon[Codec[Node]]

  private def encode(node: Node): Json = codec(node)

  private def decode(json: Json): Node = codec.decodeJson(json).toOption.get

  test("1.simple text node") {
    val xml = XML.loadString("<name>alice</name>")

    val expected = Json.obj("name" -> Json.fromString("alice"))

    assert(encode(xml) == expected)
  }

  test("2.attributes with text") {
    val xml = XML.loadString("<user id=\"42\" active=\"yes\">tom</user>")

    val expected = Json.obj(
      "user" -> Json.obj(
        "@id" -> Json.fromString("42"),
        "@active" -> Json.fromString("yes"),
        "#text" -> Json.fromString("tom")
      ))

    assert(encode(xml) == expected)
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
    assert(encode(xml) == expected)
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

    assert(encode(xml) == expected)
  }

  test("5.attribute-only element includes empty #text") {
    val xml = XML.loadString("<user id=\"42\"/>")

    val expected = Json.obj(
      "user" -> Json.obj(
        "@id" -> Json.fromString("42"),
        "#text" -> Json.fromString("")
      ))

    assert(encode(xml) == expected)
  }

  test("6.top-level unsupported node throws") {
    assertThrows[RuntimeException](encode(Comment("not-an-elem")))
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

    assert(encode(xml) == expected)
  }

  test("8.root text preserves spaces but nested text is trimmed") {
    val rootText = encode(Text("  hi  "))
    val nestedText = encode(XML.loadString("<a>  hi  </a>"))

    assert(rootText == Json.fromString("  hi  "))
    assert(nestedText == Json.obj("a" -> Json.fromString("hi")))
  }

  test("9.codec decodes a simple encoded object") {
    val json = Json.obj("name" -> Json.fromString("alice"))

    val decoded = decode(json)

    assert(decoded.label == "name")
    assert(decoded.text == "alice")
  }

  test("10.codec rejects multi-field root object") {
    val json = Json.obj(
      "a" -> Json.fromString("x"),
      "b" -> Json.fromString("y")
    )

    assert(codec.decodeJson(json).isLeft)
  }

  test("11.xml-json-xml-json round trip keeps mixed-content shape") {
    val xml = XML.loadString("<root>pre<x>1</x>mid<y>2</y>post</root>")

    val encoded = encode(xml)
    val decoded = decode(encoded)

    assert(encode(decoded) == encoded)
  }

  test("12.json-xml-json round trip keeps grouped-child shape without #children") {
    val json = Json.obj(
      "users" -> Json.obj(
        "user" -> List("alice", "bob").asJson
      ))

    val decoded = decode(json)

    assert(encode(decoded) == json)
  }

  test("13.interleaved same-level elements with no text preserve order via #children") {
    // <a/><b/><a/>: two distinct labels interleaved, no text. Grouping by label alone would lose
    // that `b` sits between the two `a` elements. #children must capture the original order.
    val xml = XML.loadString("<root><a>1</a><b>2</b><a>3</a></root>")

    val expected = Json.obj(
      "root" -> Json.obj(
        "a" -> List("1", "3").asJson,
        "b" -> Json.fromString("2"),
        "#children" -> List(
          Json.obj("a" -> Json.fromString("1")),
          Json.obj("b" -> Json.fromString("2")),
          Json.obj("a" -> Json.fromString("3"))
        ).asJson
      ))

    assert(encode(xml) == expected)
  }

  test("14.interleaved multi-label element survives xml-json-xml-json round trip") {
    val xml = XML.loadString("<root><a>1</a><b>2</b><a>3</a></root>")

    val encoded = encode(xml)
    val decoded = decode(encoded)

    // decode must reconstruct the interleaved order, and re-encoding must reproduce the same JSON.
    assert(encode(decoded) == encoded)
    assert(decoded.child.collect { case e: scala.xml.Elem => e.label }.toList == List("a", "b", "a"))
  }

  test("15.single repeated label stays a grouped array without #children") {
    // Guard the format boundary: one distinct label keeps order within its array, so no #children.
    val xml = XML.loadString("<root><a>1</a><a>2</a></root>")

    val expected = Json.obj(
      "root" -> Json.obj(
        "a" -> List("1", "2").asJson
      ))

    assert(encode(xml) == expected)
  }
}
