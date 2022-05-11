package com.github.chenharryhua.nanjin.guard.observers

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.*
import com.github.chenharryhua.nanjin.common.{ChunkSize, EmailAddr}
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.translators.{Translator, UpdateTranslator}
import fs2.{INothing, Pipe, Stream}
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

import java.util.UUID
import scala.concurrent.duration.{DurationInt, FiniteDuration}

object EmailObserver {

  def apply[F[_]: Async](client: Resource[F, SimpleEmailService[F]]): SesEmailObserver[F] =
    new SesEmailObserver[F](
      client = client,
      chunkSize = ChunkSize(60),
      interval = 120.minutes,
      isNewestFirst = true,
      Translator.html[F])

  def apply[F[_]: Async](client: Resource[F, SimpleNotificationService[F]]): SnsEmailObserver[F] =
    new SnsEmailObserver[F](
      client = client,
      chunkSize = ChunkSize(60),
      interval = 120.minutes,
      isNewestFirst = true,
      Translator.html[F])

}

final class SesEmailObserver[F[_]](
  client: Resource[F, SimpleEmailService[F]],
  chunkSize: ChunkSize, // number of events in an email
  interval: FiniteDuration, // send out email every interval
  isNewestFirst: Boolean, // the latest event comes first
  translator: Translator[F, Text.TypedTag[String]])(implicit F: Async[F])
    extends UpdateTranslator[F, Text.TypedTag[String], SesEmailObserver[F]] with all {

  private[this] def copy(
    chunkSize: ChunkSize = chunkSize,
    interval: FiniteDuration = interval,
    isNewestFirst: Boolean = isNewestFirst,
    translator: Translator[F, Text.TypedTag[String]] = translator): SesEmailObserver[F] =
    new SesEmailObserver[F](client, chunkSize, interval, isNewestFirst, translator)

  def withInterval(fd: FiniteDuration): SesEmailObserver[F] = copy(interval = fd)
  def withChunkSize(cs: ChunkSize): SesEmailObserver[F]     = copy(chunkSize = cs)
  def withOldestFirst: SesEmailObserver[F]                  = copy(isNewestFirst = false)

  override def updateTranslator(
    f: Translator[F, Text.TypedTag[String]] => Translator[F, Text.TypedTag[String]]): SesEmailObserver[F] =
    copy(translator = f(translator))

  def observe(from: EmailAddr, to: NonEmptyList[EmailAddr], subject: Subject): Pipe[F, NJEvent, INothing] =
    (es: Stream[F, NJEvent]) =>
      Stream
        .resource(client)
        .flatMap(ses =>
          Stream
            .eval(F.ref[Map[UUID, ServiceStart]](Map.empty).map(r => new ObserverFinalizeMonitor(translator, r)))
            .flatMap(ofm =>
              es.evalTap(evt => ofm.monitoring(evt))
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
                    .send(EmailContent(from.value, to.map(_.value).toList.distinct, subject.value, mailBody))
                    .attempt
                    .void
                }
                .onFinalizeCase(ofm
                  .terminated(_)
                  .flatMap { events =>
                    ses
                      .send(
                        EmailContent(
                          from.value,
                          to.map(_.value).toList.distinct,
                          "Service Termination Notice",
                          html(body(events.map(hr(_)).toList)).render))
                      .attempt
                  }
                  .void)
                .drain))
}

final class SnsEmailObserver[F[_]](
  client: Resource[F, SimpleNotificationService[F]],
  chunkSize: ChunkSize,
  interval: FiniteDuration,
  isNewestFirst: Boolean,
  translator: Translator[F, Text.TypedTag[String]])(implicit F: Async[F])
    extends UpdateTranslator[F, Text.TypedTag[String], SnsEmailObserver[F]] with all {

  private[this] def copy(
    chunkSize: ChunkSize = chunkSize,
    interval: FiniteDuration = interval,
    isNewestFirst: Boolean = isNewestFirst,
    translator: Translator[F, Text.TypedTag[String]] = translator): SnsEmailObserver[F] =
    new SnsEmailObserver[F](client, chunkSize, interval, isNewestFirst, translator)

  def withInterval(fd: FiniteDuration): SnsEmailObserver[F] = copy(interval = fd)
  def withChunkSize(cs: ChunkSize): SnsEmailObserver[F]     = copy(chunkSize = cs)
  def withOldestFirst: SnsEmailObserver[F]                  = copy(isNewestFirst = false)

  override def updateTranslator(
    f: Translator[F, Text.TypedTag[String]] => Translator[F, Text.TypedTag[String]]): SnsEmailObserver[F] =
    copy(translator = f(translator))

  def observe(snsArn: SnsArn, title: Title): Pipe[F, NJEvent, INothing] = (es: Stream[F, NJEvent]) =>
    Stream
      .resource(client)
      .flatMap(sns =>
        Stream
          .eval(F.ref[Map[UUID, ServiceStart]](Map.empty).map(r => new ObserverFinalizeMonitor(translator, r)))
          .flatMap(ofm =>
            es.evalTap(ofm.monitoring)
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
                      hr(h2(title.value)),
                      text,
                      footer(hr(p(b("Events/Max: "), s"${events.size}/$chunkSize"))))).render
                }
                sns.publish(snsArn, mailBody).attempt.void
              }
              .onFinalizeCase {
                ofm
                  .terminated(_)
                  .flatMap { events =>
                    sns
                      .publish(
                        snsArn,
                        html(body(events.map(hr(_)).prepended(hr(h2("Service Termination Notice"))))).render)
                      .attempt
                  }
                  .void
              })
          .drain)

}
