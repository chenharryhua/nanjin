package com.github.chenharryhua.nanjin.guard.observers.teams

import com.github.chenharryhua.nanjin.guard.config.StackTrace
import io.circe.syntax.given
import io.circe.{Encoder, Json}

/** Microsoft Teams Adaptive Card data model.
  *
  * @see
  *   [[https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/connectors-using?tabs=cURL#send-adaptive-cards-using-an-incoming-webhook]]
  */

sealed trait CardElement

object CardElement {
  given Encoder[CardElement] = Encoder.instance {
    case t: TextBlock       => t.asJson(using TextBlock.encoder)
    case b: BolderTextBlock => b.asJson(using BolderTextBlock.encoder)
    case c: JsonBlock       => c.asJson(using JsonBlock.encoder)
    case s: StackTraceBlock => s.asJson(using StackTraceBlock.encoder)
    case f: FactSet         => f.asJson(using FactSet.encoder)
    case c: ColumnSet       => c.asJson(using ColumnSet.encoder)
    case c: Container       => c.asJson(using Container.encoder)
  }
}

final case class JsonBlock(codeSnippet: Json) extends CardElement
object JsonBlock {
  val encoder: Encoder[JsonBlock] = (cb: JsonBlock) =>
    Json.obj(
      "type" -> Json.fromString("CodeBlock"),
      "codeSnippet" -> Json.fromString(cb.codeSnippet.spaces2),
      "language" -> Json.fromString("JSON")
    )
}

final case class StackTraceBlock(stackTrace: StackTrace) extends CardElement
object StackTraceBlock {
  val encoder: Encoder[StackTraceBlock] = (stb: StackTraceBlock) =>
    val fields = List(
      "type" -> Json.fromString("TextBlock"),
      "text" -> Json.fromString(stb.stackTrace.nbspIndented),
      "wrap" -> Json.fromBoolean(true),
      "fontType" -> Json.fromString("Monospace")
    )
    Json.obj(fields*)
}

final case class BolderTextBlock(text: String) extends CardElement
object BolderTextBlock {
  val encoder: Encoder[BolderTextBlock] = (tb: BolderTextBlock) => {
    val fields = List(
      "type" -> Json.fromString("TextBlock"),
      "text" -> Json.fromString(tb.text + ":"),
      "wrap" -> Json.fromBoolean(true),
      "fontType" -> Json.fromString("Monospace"),
      "weight" -> Json.fromString("Bolder")
    )
    Json.obj(fields*)
  }
}

final case class TextBlock(
  text: String,
  color: String = "Default",
  weight: Option[String] = None,
  size: Option[String] = None)
    extends CardElement

object TextBlock {
  val encoder: Encoder[TextBlock] = (tb: TextBlock) => {
    val fields = List(
      "type" -> Json.fromString("TextBlock"),
      "text" -> Json.fromString(tb.text),
      "color" -> Json.fromString(tb.color),
      "wrap" -> Json.fromBoolean(true),
      "fontType" -> Json.fromString("Monospace")
    ) ++
      tb.weight.map("weight" -> Json.fromString(_)) ++
      tb.size.map("size" -> Json.fromString(_))

    Json.obj(fields*)
  }
}

final case class Fact(title: String, value: String)

object Fact {
  given Encoder[Fact] = (f: Fact) =>
    Json.obj("title" -> Json.fromString(f.title + ":"), "value" -> Json.fromString(f.value))
}

final case class FactSet(facts: List[Fact]) extends CardElement

object FactSet {
  val encoder: Encoder[FactSet] = (fs: FactSet) =>
    Json.obj("type" -> Json.fromString("FactSet"), "facts" -> fs.facts.asJson)
}

final case class Column(items: List[CardElement], width: String = "stretch")

object Column {
  given Encoder[Column] = (c: Column) =>
    Json.obj(
      "type" -> Json.fromString("Column"),
      "width" -> Json.fromString(c.width),
      "items" -> c.items.asJson
    )
}

final case class ColumnSet(columns: List[Column]) extends CardElement

object ColumnSet {
  val encoder: Encoder[ColumnSet] = (cs: ColumnSet) =>
    Json.obj("type" -> Json.fromString("ColumnSet"), "columns" -> cs.columns.asJson)
}

final case class Container(items: List[CardElement], style: Option[String] = None) extends CardElement

object Container {
  val encoder: Encoder[Container] = (c: Container) => {
    val fields = List(
      "type" -> Json.fromString("Container"),
      "items" -> c.items.asJson
    ) ++ c.style.map("style" -> Json.fromString(_))
    Json.obj(fields*)
  }
}

/** The complete Adaptive Card payload for Teams webhook. */
final case class AdaptiveCard(body: List[CardElement]) {
  def appendElement(elem: CardElement): AdaptiveCard = copy(body = body :+ elem)
}

object AdaptiveCard {
  given Encoder[AdaptiveCard] = (card: AdaptiveCard) =>
    Json.obj(
      "type" -> Json.fromString("message"),
      "attachments" -> Json.arr(
        Json.obj(
          "contentType" -> Json.fromString("application/vnd.microsoft.card.adaptive"),
          "content" -> Json.obj(
            "$schema" -> Json.fromString("https://adaptivecards.io/schemas/adaptive-card.json"),
            "type" -> Json.fromString("AdaptiveCard"),
            "version" -> Json.fromString("1.5"),
            "msteams" -> Json.obj("width" -> Json.fromString("Full")),
            "body" -> card.body.asJson
          )
        )
      )
    )
}
