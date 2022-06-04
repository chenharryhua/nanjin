package com.github.chenharryhua.nanjin.guard.translators

import cats.Show
import cats.derived.auto.show.*
import io.circe.Encoder
import io.circe.generic.JsonCodec
import io.circe.generic.auto.*
import io.circe.literal.JsonStringContext

final case class TextField(tag: String, value: String)
object TextField {
  implicit val encodeTextField: Encoder[TextField] = tf => {
    val str = s"*${tf.tag}*\n${tf.value}"
    json"""
        {
           "type": "mrkdwn",
           "text": $str
        }
        """
  }
}
// slack format
sealed trait Section
object Section {
  implicit val encodeSection: Encoder[Section] = Encoder.instance {
    case JuxtaposeSection(first, second) =>
      json"""
            {
               "type": "section",
               "fields": ${List(first, second)}
            }
            """
    case MarkdownSection(text) =>
      json"""
            {
               "type": "section",
               "text": {
                          "type": "mrkdwn",
                          "text": $text
                       }
            }
            """
    case KeyValueSection(key, value) =>
      json"""
            {
               "type": "section",
               "text": ${TextField(key, value)}
            }
            """
  }
}

final case class JuxtaposeSection(first: TextField, second: TextField) extends Section
final case class KeyValueSection(tag: String, value: String) extends Section
final case class MarkdownSection(text: String) extends Section

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
