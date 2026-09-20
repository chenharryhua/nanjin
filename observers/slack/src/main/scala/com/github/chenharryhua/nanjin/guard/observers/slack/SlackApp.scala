package com.github.chenharryhua.nanjin.guard.observers.slack

import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.guard.translator.{TextEntry, Translator}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.http4s.Uri
import org.http4s.client.Client

final private case class TextField(tag: String, value: String)
private object TextField {
  def apply(te: TextEntry): TextField = TextField(te.tag, te.text)

  given Encoder[TextField] = tf => {
    val str = s"*${tf.tag}*\n${tf.value}"
    Json.obj("type" -> Json.fromString("mrkdwn"), "text" -> Json.fromString(str))
  }
}
// slack format
sealed private trait Section
private object Section {
  given Encoder[Section] = Encoder.instance {
    case JuxtaposeSection(first, second) =>
      Json.obj("type" -> Json.fromString("section"), "fields" -> List(first, second).asJson)

    case MarkdownSection(text) =>
      Json.obj(
        "type" -> Json.fromString("section"),
        "text" -> Json.obj("type" -> Json.fromString("mrkdwn"), "text" -> Json.fromString(text)))

    case TagValueSection(tag, value) =>
      Json.obj("type" -> Json.fromString("section"), "text" -> TextField(tag, value).asJson)

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

final private case class JuxtaposeSection(first: TextField, second: TextField) extends Section derives Encoder
final private case class TagValueSection(tag: String, value: String) extends Section derives Encoder
final private case class MarkdownSection(text: String) extends Section derives Encoder
final private case class HeaderSection(text: String) extends Section derives Encoder

final private case class Attachment(color: String, blocks: List[Section]) derives Encoder

final private case class SlackApp(
  username: String,
  attachments: List[Attachment],
  icon_url: Option[String] = None)
    derives Encoder

final private case class Params[F[_]](
  client: Resource[F, Client[F]],
  translator: Translator[F, SlackApp],
  maxStackTraceFrames: Option[Int],
  icon_url: Option[Uri]
)
