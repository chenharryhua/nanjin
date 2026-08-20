package com.github.chenharryhua.nanjin.guard.observers.teams

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
    case t: TextBlock => t.asJson(using TextBlock.encoder)
    case f: FactSet   => f.asJson(using FactSet.encoder)
    case c: CodeBlock => c.asJson(using CodeBlock.encoder)
    case c: ColumnSet => c.asJson(using ColumnSet.encoder)
    case c: Container => c.asJson(using Container.encoder)
  }
}

final case class TextBlock(
  text: String,
  weight: Option[String] = None,
  size: Option[String] = None,
  color: Option[String] = None,
  wrap: Boolean = true)
    extends CardElement

object TextBlock {
  val encoder: Encoder[TextBlock] = (tb: TextBlock) => {
    val fields = List(
      "type" -> Json.fromString("TextBlock"),
      "text" -> Json.fromString(tb.text),
      "wrap" -> Json.fromBoolean(tb.wrap)
    ) ++
      tb.weight.map("weight" -> Json.fromString(_)) ++
      tb.size.map("size" -> Json.fromString(_)) ++
      tb.color.map("color" -> Json.fromString(_))

    Json.obj(fields*)
  }
}

final case class Fact(title: String, value: String)

object Fact {
  given Encoder[Fact] = (f: Fact) =>
    Json.obj("title" -> Json.fromString(f.title), "value" -> Json.fromString(f.value))
}

final case class FactSet(facts: List[Fact]) extends CardElement

object FactSet {
  val encoder: Encoder[FactSet] = (fs: FactSet) =>
    Json.obj("type" -> Json.fromString("FactSet"), "facts" -> fs.facts.asJson)
}

final case class CodeBlock(codeSnippet: String, language: String = "text") extends CardElement

object CodeBlock {
  val encoder: Encoder[CodeBlock] = (cb: CodeBlock) =>
    Json.obj(
      "type" -> Json.fromString("CodeBlock"),
      "codeSnippet" -> Json.fromString(cb.codeSnippet),
      "language" -> Json.fromString(cb.language)
    )
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
final case class AdaptiveCard(body: List[CardElement], themeColor: String) {
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
            "$schema" -> Json.fromString("http://adaptivecards.io/schemas/adaptive-card.json"),
            "type" -> Json.fromString("AdaptiveCard"),
            "version" -> Json.fromString("1.5"),
            "msteams" -> Json.obj("width" -> Json.fromString("Full")),
            "body" -> card.body.asJson
          )
        )
      ),
      "themeColor" -> Json.fromString(card.themeColor)
    )
}
