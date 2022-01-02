package com.github.chenharryhua.nanjin.guard.translators

import cats.Monad
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import io.circe.Encoder
import io.circe.generic.auto.*
import io.circe.literal.JsonStringContext

import scala.concurrent.duration.FiniteDuration

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
final case class SlackApp(username: String, attachments: List[Attachment])

final case class SlackConfig[F[_]: Monad](
  reportInterval: Option[FiniteDuration]
) {

  def withReportInterval(fd: FiniteDuration): SlackConfig[F] = copy(reportInterval = Some(fd))
  def withoutReportInterval: SlackConfig[F]                  = copy(reportInterval = None)

}
