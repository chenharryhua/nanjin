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

object SesEmailObserver {

  def apply[F[_]: Async](from: EmailAddr, to: NonEmptyList[EmailAddr]): SesEmailObserver[F] =
    new SesEmailObserver[F](
      client = ses[F],
      from = from,
      to = to,
      subject = None,
      chunkSize = ChunkSize(60),
      interval = 60.minutes,
      isNewestFirst = true,
      Translator.html[F])
}

object SnsEmailObserver {

  def apply[F[_]: Async](client: Resource[F, SimpleNotificationService[F]]): SnsEmailObserver[F] =
    new SnsEmailObserver[F](
      client = client,
      title = None,
      chunkSize = ChunkSize(60),
      interval = 60.minutes,
      isNewestFirst = true,
      Translator.html[F])

  def apply[F[_]: Async](snsArn: SnsArn): SnsEmailObserver[F] = apply[F](sns(snsArn))
}

final class SesEmailObserver[F[_]](
  client: Resource[F, SimpleEmailService[F]],
  from: EmailAddr,
  to: NonEmptyList[EmailAddr],
  subject: Option[Subject],
  chunkSize: ChunkSize, // number of events in an email
  interval: FiniteDuration, // send out email every interval
  isNewestFirst: Boolean, // the latest event comes first
  translator: Translator[F, Text.TypedTag[String]])(implicit F: Async[F])
    extends Pipe[F, NJEvent, INothing] with UpdateTranslator[F, Text.TypedTag[String], SesEmailObserver[F]] with all {

  private[this] def copy(
    client: Resource[F, SimpleEmailService[F]] = client,
    subject: Option[Subject] = subject,
    chunkSize: ChunkSize = chunkSize,
    interval: FiniteDuration = interval,
    isNewestFirst: Boolean = isNewestFirst,
    translator: Translator[F, Text.TypedTag[String]] = translator): SesEmailObserver[F] =
    new SesEmailObserver[F](client, from, to, subject, chunkSize, interval, isNewestFirst, translator)

  def withInterval(fd: FiniteDuration): SesEmailObserver[F] = copy(interval = fd)
  def withChunkSize(cs: ChunkSize): SesEmailObserver[F]     = copy(chunkSize = cs)
  def withSubject(sj: Subject): SesEmailObserver[F]         = copy(subject = Some(sj))
  def withOldestFirst: SesEmailObserver[F]                  = copy(isNewestFirst = false)

  def withClient(client: Resource[F, SimpleEmailService[F]]): SesEmailObserver[F] = copy(client = client)

  override def updateTranslator(
    f: Translator[F, Text.TypedTag[String]] => Translator[F, Text.TypedTag[String]]): SesEmailObserver[F] =
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

final class SnsEmailObserver[F[_]](
  client: Resource[F, SimpleNotificationService[F]],
  title: Option[Title],
  chunkSize: ChunkSize,
  interval: FiniteDuration,
  isNewestFirst: Boolean,
  translator: Translator[F, Text.TypedTag[String]])(implicit F: Async[F])
    extends Pipe[F, NJEvent, INothing] with UpdateTranslator[F, Text.TypedTag[String], SnsEmailObserver[F]] with all {

  private[this] def copy(
    client: Resource[F, SimpleNotificationService[F]] = client,
    title: Option[Title] = title,
    chunkSize: ChunkSize = chunkSize,
    interval: FiniteDuration = interval,
    isNewestFirst: Boolean = isNewestFirst,
    translator: Translator[F, Text.TypedTag[String]] = translator): SnsEmailObserver[F] =
    new SnsEmailObserver[F](client, title, chunkSize, interval, isNewestFirst, translator)

  def withInterval(fd: FiniteDuration): SnsEmailObserver[F] = copy(interval = fd)
  def withChunkSize(cs: ChunkSize): SnsEmailObserver[F]     = copy(chunkSize = cs)
  def withTitle(t: Title): SnsEmailObserver[F]              = copy(title = Some(t))
  def withOldestFirst: SnsEmailObserver[F]                  = copy(isNewestFirst = false)

  override def updateTranslator(
    f: Translator[F, Text.TypedTag[String]] => Translator[F, Text.TypedTag[String]]): SnsEmailObserver[F] =
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
