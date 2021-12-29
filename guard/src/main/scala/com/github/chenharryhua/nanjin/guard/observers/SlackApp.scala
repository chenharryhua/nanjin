package com.github.chenharryhua.nanjin.guard.observers

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
  goodColor: String,
  warnColor: String,
  infoColor: String,
  errorColor: String,
  metricsReportEmoji: String,
  startActionEmoji: String,
  succActionEmoji: String,
  failActionEmoji: String,
  retryActionEmoji: String,
  durationFormatter: DurationFormatter,
  reportInterval: Option[FiniteDuration],
  extraSlackSections: F[List[Section]],
  isLoggging: Boolean,
  supporters: List[String]
) {
  val atSupporters: String =
    supporters
      .filter(_.nonEmpty)
      .map(_.trim)
      .map(spt => if (spt.startsWith("@") || spt.startsWith("<")) spt else s"@$spt")
      .distinct
      .mkString(" ")

  def withColorGood(color: String): SlackConfig[F]  = copy(goodColor = color)
  def withColorWarn(color: String): SlackConfig[F]  = copy(warnColor = color)
  def withColorInfo(color: String): SlackConfig[F]  = copy(infoColor = color)
  def withColorError(color: String): SlackConfig[F] = copy(errorColor = color)

  def withEmojiMetricsReport(emoji: String): SlackConfig[F] = copy(metricsReportEmoji = emoji)
  def withEmojiStartAction(emoji: String): SlackConfig[F]   = copy(startActionEmoji = emoji)
  def withEmojiSuccAction(emoji: String): SlackConfig[F]    = copy(succActionEmoji = emoji)
  def withEmojiFailAction(emoji: String): SlackConfig[F]    = copy(failActionEmoji = emoji)
  def withEmojiRetryAction(emoji: String): SlackConfig[F]   = copy(retryActionEmoji = emoji)

  def at(supporter: String): SlackConfig[F]        = copy(supporters = supporter :: supporters)
  def at(supporters: List[String]): SlackConfig[F] = copy(supporters = supporters ::: supporters)

  def withSection(value: F[String]): SlackConfig[F] =
    copy(extraSlackSections = for {
      esf <- extraSlackSections
      v <- value
    } yield esf :+ MarkdownSection(abbreviate(v)))

  def withSection(value: String): SlackConfig[F] = withSection(Monad[F].pure(value))

  def withDurationFormatter(fmt: DurationFormatter): SlackConfig[F] = copy(durationFormatter = fmt)

  def withReportInterval(fd: FiniteDuration): SlackConfig[F] = copy(reportInterval = Some(fd))
  def withoutReportInterval: SlackConfig[F]                  = copy(reportInterval = None)

  def withLogging: SlackConfig[F] = copy(isLoggging = true)

}
