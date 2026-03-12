package com.github.chenharryhua.nanjin.guard.observers.sns

import com.github.chenharryhua.nanjin.guard.translator.TextEntry
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import io.circe.Codec

final case class TextField(tag: String, value: String)
object TextField {
  def apply(te: TextEntry): TextField = TextField(te.tag, te.text)

  given Encoder[TextField] = tf => {
    val str = s"*${tf.tag}*\n${tf.value}"
    Json.obj("type" -> Json.fromString("mrkdwn"), "text" -> Json.fromString(str))
  }
}
// slack format
sealed trait Section
object Section {
  given Encoder[Section] = Encoder.instance {
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

final case class JuxtaposeSection(first: TextField, second: TextField) extends Section derives Codec.AsObject
final case class KeyValueSection(tag: String, value: String) extends Section derives Codec.AsObject
final case class MarkdownSection(text: String) extends Section derives Codec.AsObject
final case class HeaderSection(text: String) extends Section derives Codec.AsObject

final case class Attachment(color: String, blocks: List[Section]) derives Codec.AsObject

final case class SlackApp(username: String, attachments: List[Attachment]) derives Codec.AsObject {
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
