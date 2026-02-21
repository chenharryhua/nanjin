package com.github.chenharryhua.nanjin.guard.observers.sns

import cats.Show
import cats.derived.auto.show.*
import com.github.chenharryhua.nanjin.guard.translator.TextEntry
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}

final case class TextField(tag: String, value: String)
object TextField {
  def apply(te: TextEntry): TextField = TextField(te.tag, te.text)

  implicit val encodeTextField: Encoder[TextField] = tf => {
    val str = s"*${tf.tag}*\n${tf.value}"
    Json.obj("type" -> Json.fromString("mrkdwn"), "text" -> Json.fromString(str))
  }
}
// slack format
sealed trait Section
object Section {
  implicit val encodeSection: Encoder[Section] = Encoder.instance {
    case JuxtaposeSection(first, second) =>
      Json.obj("type" -> Json.fromString("section"), "fields" -> List(first, second).asJson)

    case MarkdownSection(text) =>
      Json.obj(
        "type" -> Json.fromString("section"),
        "text" -> Json.obj("type" -> Json.fromString("mrkdwn"), "text" -> Json.fromString(text)))

    case KeyValueSection(key, value) =>
      Json.obj("type" -> Json.fromString("section"), "text" -> TextField(key, value).asJson)

    case HeaderSection(text) =>
      Json.obj(
        "type" -> Json.fromString("header"),
        "text" -> Json.obj(
          "type" -> Json.fromString("plain_text"),
          "text" -> Json.fromString(text),
          "emoji" -> Json.fromBoolean(true))
      )
  }
}

final case class JuxtaposeSection(first: TextField, second: TextField) extends Section
final case class KeyValueSection(tag: String, value: String) extends Section
final case class MarkdownSection(text: String) extends Section
final case class HeaderSection(text: String) extends Section

final case class Attachment(color: String, blocks: List[Section])

@JsonCodec
final case class SlackApp(username: String, attachments: List[Attachment]) {
  // before first section
  def prependMarkdown(text: String): SlackApp =
    SlackApp(
      username,
      attachments match {
        case Nil          => Nil
        case head :: rest => Attachment(head.color, MarkdownSection(text) :: head.blocks) :: rest
      })

  // after last section
  def appendMarkdown(text: String): SlackApp = {
    val updated = attachments.reverse match {
      case Nil          => Nil
      case head :: rest => Attachment(head.color, head.blocks ::: List(MarkdownSection(text))) :: rest
    }
    SlackApp(username, updated.reverse)
  }
}

object SlackApp {
  implicit val showSlackApp: Show[SlackApp] = cats.derived.semiauto.show[SlackApp]
}
