package com.github.chenharryhua.nanjin.guard.observers

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.*
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.common.{ChunkSize, EmailAddr}
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.translators.{Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object sesEmail {

  def apply[F[_]: Async](from: EmailAddr, to: NonEmptyList[EmailAddr]): NJSesEmailObserver[F] =
    new NJSesEmailObserver[F](
      client = ses[F],
      from = from,
      to = to,
      subject = None,
      chunkSize = ChunkSize(60),
      interval = 60.minutes,
      isNewestFirst = true,
      Translator.html[F])
}

object snsEmail {

  def apply[F[_]: Async](client: Resource[F, SimpleNotificationService[F]]): NJSnsEmailObserver[F] =
    new NJSnsEmailObserver[F](
      client = client,
      title = None,
      chunkSize = ChunkSize(60),
      interval = 60.minutes,
      isNewestFirst = true,
      Translator.html[F])

  def apply[F[_]: Async](snsArn: SnsArn): NJSnsEmailObserver[F] = apply[F](sns(snsArn))
}

final class NJSesEmailObserver[F[_]: Async](
  client: Resource[F, SimpleEmailService[F]],
  from: EmailAddr,
  to: NonEmptyList[EmailAddr],
  subject: Option[Subject],
  chunkSize: ChunkSize, // number of events in an email
  interval: FiniteDuration, // send out email every interval
  isNewestFirst: Boolean, // the latest event comes first
  translator: Translator[F, Text.TypedTag[String]]
) extends Pipe[F, NJEvent, String] with UpdateTranslator[F, Text.TypedTag[String], NJSesEmailObserver[F]] with all {

  private[this] def copy(
    client: Resource[F, SimpleEmailService[F]] = client,
    subject: Option[Subject] = subject,
    chunkSize: ChunkSize = chunkSize,
    interval: FiniteDuration = interval,
    isNewestFirst: Boolean = isNewestFirst,
    translator: Translator[F, Text.TypedTag[String]] = translator): NJSesEmailObserver[F] =
    new NJSesEmailObserver[F](client, from, to, subject, chunkSize, interval, isNewestFirst, translator)

  def withInterval(fd: FiniteDuration): NJSesEmailObserver[F] = copy(interval = fd)
  def withChunkSize(cs: ChunkSize): NJSesEmailObserver[F]     = copy(chunkSize = cs)
  def withSubject(sj: Subject): NJSesEmailObserver[F]         = copy(subject = Some(sj))
  def withOldestFirst: NJSesEmailObserver[F]                  = copy(isNewestFirst = false)

  def withClient(client: Resource[F, SimpleEmailService[F]]): NJSesEmailObserver[F] = copy(client = client)

  override def updateTranslator(
    f: Translator[F, Text.TypedTag[String]] => Translator[F, Text.TypedTag[String]]): NJSesEmailObserver[F] =
    copy(translator = f(translator))

  override def apply(es: Stream[F, NJEvent]): Stream[F, String] =
    for {
      c <- Stream.resource(client)
      mb <- es
        .evalMap(e =>
          translator.filter {
            case event: ActionEvent => event.actionParams.isNonTrivial
            case _                  => true
          }.translate(e))
        .unNone
        .groupWithin(chunkSize.value, interval)
        .evalMap { events =>
          val mailBody: String = {
            val text: List[Text.TypedTag[String]] =
              if (isNewestFirst) events.map(hr(_)).toList.reverse else events.map(hr(_)).toList
            html(body(text, footer(hr(p(b("Events/Max: "), s"${events.size}/$chunkSize"))))).render
          }

          c.send(
            EmailContent(from.value, to.map(_.value).toList.distinct, subject.map(_.value).getOrElse(""), mailBody))
            .attempt
            .as(mailBody)
        }
    } yield mb
}

final class NJSnsEmailObserver[F[_]: Async](
  client: Resource[F, SimpleNotificationService[F]],
  title: Option[Title],
  chunkSize: ChunkSize,
  interval: FiniteDuration,
  isNewestFirst: Boolean,
  translator: Translator[F, Text.TypedTag[String]]
) extends Pipe[F, NJEvent, String] with UpdateTranslator[F, Text.TypedTag[String], NJSnsEmailObserver[F]] with all {

  private[this] def copy(
    client: Resource[F, SimpleNotificationService[F]] = client,
    title: Option[Title] = title,
    chunkSize: ChunkSize = chunkSize,
    interval: FiniteDuration = interval,
    isNewestFirst: Boolean = isNewestFirst,
    translator: Translator[F, Text.TypedTag[String]] = translator): NJSnsEmailObserver[F] =
    new NJSnsEmailObserver[F](client, title, chunkSize, interval, isNewestFirst, translator)

  def withInterval(fd: FiniteDuration): NJSnsEmailObserver[F] = copy(interval = fd)
  def withChunkSize(cs: ChunkSize): NJSnsEmailObserver[F]     = copy(chunkSize = cs)
  def withTitle(t: Title): NJSnsEmailObserver[F]              = copy(title = Some(t))
  def withOldestFirst: NJSnsEmailObserver[F]                  = copy(isNewestFirst = false)

  override def updateTranslator(
    f: Translator[F, Text.TypedTag[String]] => Translator[F, Text.TypedTag[String]]): NJSnsEmailObserver[F] =
    copy(translator = f(translator))

  override def apply(es: Stream[F, NJEvent]): Stream[F, String] =
    for {
      c <- Stream.resource(client)
      rst <- es
        .evalMap(e =>
          translator.filter {
            case event: ActionEvent => event.actionParams.isNonTrivial
            case _                  => true
          }.translate(e))
        .unNone
        .groupWithin(chunkSize.value, interval)
        .evalMap { events =>
          val mailBody: String = {
            val text: List[Text.TypedTag[String]] =
              if (isNewestFirst) events.map(hr(_)).toList.reverse else events.map(hr(_)).toList
            html(
              body(
                text.prependedAll(title.map(t => hr(h2(t.value)))),
                footer(hr(p(b("Events/Max: "), s"${events.size}/$chunkSize"))))).render
          }
          c.publish(mailBody).attempt.as(mailBody)
        }
    } yield rst
}
