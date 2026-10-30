package com.github.chenharryhua.nanjin.guard.observers.teams

import com.github.chenharryhua.nanjin.guard.config.StackTrace
import io.circe.Json
import io.circe.syntax.EncoderOps
import munit.FunSuite

class AdaptiveCardTest extends FunSuite {

  private def encoded(element: CardElement): Json = element.asJson

  test("1.TextBlock emits required fields and optional weight/size only when present") {
    val plain = encoded(TextBlock("plain"))
    val decorated =
      encoded(TextBlock("title", color = "Attention", weight = Some("Bolder"), size = Some("Large")))

    assertEquals(plain.hcursor.get[String]("type").toOption, Some("TextBlock"))
    assertEquals(plain.hcursor.get[String]("text").toOption, Some("plain"))
    assertEquals(plain.hcursor.get[String]("color").toOption, Some("Default"))
    assert(plain.hcursor.downField("weight").focus.isEmpty)
    assert(plain.hcursor.downField("size").focus.isEmpty)

    assertEquals(decorated.hcursor.get[String]("color").toOption, Some("Attention"))
    assertEquals(decorated.hcursor.get[String]("weight").toOption, Some("Bolder"))
    assertEquals(decorated.hcursor.get[String]("size").toOption, Some("Large"))
  }

  test("2.BolderTextBlock, JsonBlock, and StackTraceBlock retain their presentation contracts") {
    val bold = encoded(BolderTextBlock("Details"))
    val code = encoded(JsonBlock(Json.obj("answer" -> Json.fromInt(42))))
    val trace = StackTrace(new RuntimeException("teams-stack"))
    val stack = encoded(StackTraceBlock(trace))

    assertEquals(bold.hcursor.get[String]("text").toOption, Some("Details:"))
    assertEquals(bold.hcursor.get[String]("weight").toOption, Some("Bolder"))
    assertEquals(code.hcursor.get[String]("type").toOption, Some("CodeBlock"))
    assert(code.hcursor.get[String]("codeSnippet").toOption.exists(_.contains("\"answer\" : 42")))
    assertEquals(code.hcursor.get[String]("language").toOption, Some("JSON"))
    assertEquals(stack.hcursor.get[String]("fontType").toOption, Some("Monospace"))
    assert(stack.hcursor.get[String]("text").toOption.exists(_.contains("teams-stack")))
  }

  test("3.FactSet, ColumnSet, and Container encode nested card elements in order") {
    val facts = FactSet(List(Fact("Status", "healthy"), Fact("Count", "2")))
    val columns =
      ColumnSet(List(Column(List(TextBlock("left")), width = "auto"), Column(List(BolderTextBlock("right")))))
    val plain_container = Container(List(facts))
    val styled_container = Container(List(columns), style = Some("emphasis"))

    val facts_json = encoded(facts)
    val columns_json = encoded(columns)
    val plain_json = encoded(plain_container)
    val styled_json = encoded(styled_container)

    assertEquals(
      facts_json.hcursor.downField("facts").downArray.get[String]("title").toOption,
      Some("Status:"))
    assertEquals(
      facts_json.hcursor.downField("facts").downArray.get[String]("value").toOption,
      Some("healthy"))
    assertEquals(
      columns_json.hcursor.downField("columns").downArray.get[String]("width").toOption,
      Some("auto"))
    assert(plain_json.hcursor.downField("style").focus.isEmpty)
    assertEquals(styled_json.hcursor.get[String]("style").toOption, Some("emphasis"))
  }

  test("4.appendElement returns a new card and preserves body order") {
    val original = AdaptiveCard(List(TextBlock("first")))
    val appended = original.appendElement(BolderTextBlock("second"))

    assertEquals(original.body.size, 1)
    assertEquals(appended.body.size, 2)
    assertEquals(
      appended.body.map(encoded).flatMap(_.hcursor.get[String]("text").toOption),
      List("first", "second:"))
  }

  test("5.AdaptiveCard encoder emits the complete Teams message envelope") {
    val card = AdaptiveCard(List(TextBlock("body"))).asJson
    val attachment = card.hcursor.downField("attachments").downArray
    val content = attachment.downField("content")

    assertEquals(card.hcursor.get[String]("type").toOption, Some("message"))
    assertEquals(
      attachment.get[String]("contentType").toOption,
      Some("application/vnd.microsoft.card.adaptive"))
    assertEquals(
      content.get[String]("$schema").toOption,
      Some("https://adaptivecards.io/schemas/adaptive-card.json"))
    assertEquals(content.get[String]("type").toOption, Some("AdaptiveCard"))
    assertEquals(content.get[String]("version").toOption, Some("1.5"))
    assertEquals(content.downField("msteams").get[String]("width").toOption, Some("Full"))
    assertEquals(content.downField("body").downArray.get[String]("text").toOption, Some("body"))
  }
}
