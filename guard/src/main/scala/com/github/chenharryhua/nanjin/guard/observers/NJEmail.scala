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

  def apply[F[_]: Async](from: EmailAddr, to: NonEmptyList[EmailAddr]): NJSesEmail[F] =
    new NJSesEmail[F](
      client = ses[F],
      from = from,
      to = to,
      subject = None,
      chunkSize = ChunkSize(60),
      interval = 60.minutes,
      Translator.html[F])
}

object snsEmail {

  def apply[F[_]: Async](client: Resource[F, SimpleNotificationService[F]]): NJSnsEmail[F] =
    new NJSnsEmail[F](
      client = client,
      title = None,
      chunkSize = ChunkSize(60),
      interval = 60.minutes,
      Translator.html[F])

  def apply[F[_]: Async](snsArn: SnsArn): NJSnsEmail[F] = apply[F](sns(snsArn))
}

final class NJSesEmail[F[_]: Async](
  client: Resource[F, SimpleEmailService[F]],
  from: EmailAddr,
  to: NonEmptyList[EmailAddr],
  subject: Option[Subject],
  chunkSize: ChunkSize,
  interval: FiniteDuration,
  translator: Translator[F, Text.TypedTag[String]]
) extends Pipe[F, NJEvent, String] with UpdateTranslator[F, Text.TypedTag[String], NJSesEmail[F]] with all {

  private[this] def copy(
    client: Resource[F, SimpleEmailService[F]] = client,
    subject: Option[Subject] = subject,
    chunkSize: ChunkSize = chunkSize,
    interval: FiniteDuration = interval,
    translator: Translator[F, Text.TypedTag[String]] = translator): NJSesEmail[F] =
    new NJSesEmail[F](client, from, to, subject, chunkSize, interval, translator)

  def withInterval(fd: FiniteDuration): NJSesEmail[F] = copy(interval = fd)
  def withChunkSize(cs: ChunkSize): NJSesEmail[F]     = copy(chunkSize = cs)
  def withSubject(sj: Subject): NJSesEmail[F]         = copy(subject = Some(sj))

  def withClient(client: Resource[F, SimpleEmailService[F]]): NJSesEmail[F] = copy(client = client)

  override def updateTranslator(
    f: Translator[F, Text.TypedTag[String]] => Translator[F, Text.TypedTag[String]]): NJSesEmail[F] =
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
          val mailBody: String =
            html(body(events.map(hr(_)).toList, footer(hr(p(b("Events/Max: "), s"${events.size}/$chunkSize"))))).render
          c.send(
            EmailContent(from.value, to.map(_.value).toList.distinct, subject.map(_.value).getOrElse(""), mailBody))
            .attempt
            .as(mailBody)
        }
    } yield mb
}

final class NJSnsEmail[F[_]: Async](
  client: Resource[F, SimpleNotificationService[F]],
  title: Option[Title],
  chunkSize: ChunkSize,
  interval: FiniteDuration,
  translator: Translator[F, Text.TypedTag[String]]
) extends Pipe[F, NJEvent, String] with UpdateTranslator[F, Text.TypedTag[String], NJSnsEmail[F]] with all {

  private[this] def copy(
    client: Resource[F, SimpleNotificationService[F]] = client,
    title: Option[Title] = title,
    chunkSize: ChunkSize = chunkSize,
    interval: FiniteDuration = interval,
    translator: Translator[F, Text.TypedTag[String]] = translator): NJSnsEmail[F] =
    new NJSnsEmail[F](client, title, chunkSize, interval, translator)

  def withInterval(fd: FiniteDuration): NJSnsEmail[F] = copy(interval = fd)
  def withChunkSize(cs: ChunkSize): NJSnsEmail[F]     = copy(chunkSize = cs)
  def withTitle(t: Title): NJSnsEmail[F]              = copy(title = Some(t))

  override def updateTranslator(
    f: Translator[F, Text.TypedTag[String]] => Translator[F, Text.TypedTag[String]]): NJSnsEmail[F] =
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
          val mailBody: String =
            html(
              body(
                events.map(hr(_)).toList.prependedAll(title.map(t => hr(h2(t.value)))),
                footer(hr(p(b("Events/Max: "), s"${events.size}/$chunkSize"))))).render
          c.publish(mailBody).attempt.as(mailBody)
        }
    } yield rst
}
