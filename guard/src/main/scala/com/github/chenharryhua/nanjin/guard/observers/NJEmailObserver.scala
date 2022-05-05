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
import fs2.{INothing, Pipe, Stream}
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

import java.util.UUID
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

final class NJSesEmailObserver[F[_]](
  client: Resource[F, SimpleEmailService[F]],
  from: EmailAddr,
  to: NonEmptyList[EmailAddr],
  subject: Option[Subject],
  chunkSize: ChunkSize, // number of events in an email
  interval: FiniteDuration, // send out email every interval
  isNewestFirst: Boolean, // the latest event comes first
  translator: Translator[F, Text.TypedTag[String]])(implicit F: Async[F])
    extends Pipe[F, NJEvent, INothing] with UpdateTranslator[F, Text.TypedTag[String], NJSesEmailObserver[F]] with all {

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

  override def apply(es: Stream[F, NJEvent]): Stream[F, INothing] =
    Stream
      .resource(client)
      .flatMap(ses =>
        Stream
          .eval(F.ref[Map[UUID, ServiceStart]](Map.empty))
          .flatMap(ref =>
            es.evalTap(evt => updateRef(ref, evt))
              .evalMap(e =>
                translator.filter {
                  case event: ActionEvent => event.actionParams.isNonTrivial
                  case _                  => true
                }.translate(e))
              .unNone
              .groupWithin(chunkSize.value, interval)
              .evalTap { events =>
                val mailBody: String = {
                  val text: List[Text.TypedTag[String]] =
                    if (isNewestFirst) events.map(hr(_)).toList.reverse else events.map(hr(_)).toList
                  html(body(text, footer(hr(p(b("Events/Max: "), s"${events.size}/$chunkSize"))))).render
                }
                ses
                  .send(
                    EmailContent(
                      from.value,
                      to.map(_.value).toList.distinct,
                      subject.map(_.value).getOrElse(""),
                      mailBody))
                  .attempt
                  .void
              }
              .onFinalize(serviceTerminateEvents(ref, translator).flatMap { events =>
                ses
                  .send(
                    EmailContent(
                      from.value,
                      to.map(_.value).toList.distinct,
                      AbnormalTerminationMessage,
                      html(body(events.map(hr(_)).toList)).render))
                  .attempt
              }.void)
              .drain))
}

final class NJSnsEmailObserver[F[_]](
  client: Resource[F, SimpleNotificationService[F]],
  title: Option[Title],
  chunkSize: ChunkSize,
  interval: FiniteDuration,
  isNewestFirst: Boolean,
  translator: Translator[F, Text.TypedTag[String]])(implicit F: Async[F])
    extends Pipe[F, NJEvent, INothing] with UpdateTranslator[F, Text.TypedTag[String], NJSnsEmailObserver[F]] with all {

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

  override def apply(es: Stream[F, NJEvent]): Stream[F, INothing] =
    Stream
      .resource(client)
      .flatMap(sns =>
        Stream
          .eval(F.ref[Map[UUID, ServiceStart]](Map.empty))
          .flatMap(ref =>
            es.evalTap(evt => updateRef(ref, evt))
              .evalMap(e =>
                translator.filter {
                  case event: ActionEvent => event.actionParams.isNonTrivial
                  case _                  => true
                }.translate(e))
              .unNone
              .groupWithin(chunkSize.value, interval)
              .evalTap { events =>
                val mailBody: String = {
                  val text: List[Text.TypedTag[String]] =
                    if (isNewestFirst) events.map(hr(_)).toList.reverse else events.map(hr(_)).toList
                  html(
                    body(
                      text.prependedAll(title.map(t => hr(h2(t.value)))),
                      footer(hr(p(b("Events/Max: "), s"${events.size}/$chunkSize"))))).render
                }
                sns.publish(mailBody).attempt.void
              }
              .onFinalize(serviceTerminateEvents(ref, translator).flatMap { events =>
                sns.publish(html(body(events.map(hr(_)).prepended(hr(h2(AbnormalTerminationMessage))))).render).attempt
              }.void))
          .drain)

}
