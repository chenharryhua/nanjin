package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.*
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.translators.{Translator, UpdateTranslator}
import fs2.{Pipe, Stream}
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object email {

  def apply[F[_]: Async](
    from: String,
    to: List[String],
    subject: String,
    client: Resource[F, SimpleEmailService[F]]): NJEmail[F] =
    new NJEmail[F](
      from = from,
      to = to,
      subject = subject,
      client = client,
      chunkSize = 60,
      interval = 60.minutes,
      Translator.html[F]
    )

  def apply[F[_]: Async](from: String, to: List[String], subject: String): NJEmail[F] =
    apply[F](from, to, subject, ses[F])

  def apply[F[_]: Async](client: Resource[F, SimpleNotificationService[F]]): NJSnsEmail[F] =
    new NJSnsEmail[F](client, 60, 60.minutes, Translator.html[F])

  def apply[F[_]: Async](snsArn: SnsArn): NJSnsEmail[F] = apply(sns(snsArn))

}

final class NJEmail[F[_]: Async] private[observers] (
  from: String,
  to: List[String],
  subject: String,
  client: Resource[F, SimpleEmailService[F]],
  chunkSize: Int,
  interval: FiniteDuration,
  translator: Translator[F, Text.TypedTag[String]]
) extends Pipe[F, NJEvent, String] with UpdateTranslator[F, Text.TypedTag[String], NJEmail[F]] with all {

  private def copy(
    chunkSize: Int = chunkSize,
    interval: FiniteDuration = interval,
    translator: Translator[F, Text.TypedTag[String]] = translator): NJEmail[F] =
    new NJEmail[F](from, to, subject, client, chunkSize, interval, translator)

  def withInterval(fd: FiniteDuration): NJEmail[F] = copy(interval = fd)
  def withChunkSize(cs: Int): NJEmail[F]           = copy(chunkSize = cs)

  override def updateTranslator(
    f: Translator[F, Text.TypedTag[String]] => Translator[F, Text.TypedTag[String]]): NJEmail[F] =
    copy(translator = f(translator))

  override def apply(es: Stream[F, NJEvent]): Stream[F, String] =
    for {
      c <- Stream.resource(client)
      mb <- es
        .evalMap(e =>
          translator.filter {
            case event: ActionEvent => event.actionInfo.nonTrivial
            case _                  => true
          }.translate(e))
        .unNone
        .groupWithin(chunkSize, interval)
        .evalMap { events =>
          val mailBody: String =
            html(body(events.map(hr(_)).toList, footer(hr(p(b("Events/Max: "), s"${events.size}/$chunkSize"))))).render
          c.send(EmailContent(from, to.distinct, subject, mailBody)).attempt.as(mailBody)
        }
    } yield mb
}

final class NJSnsEmail[F[_]: Async] private[observers] (
  client: Resource[F, SimpleNotificationService[F]],
  chunkSize: Int,
  interval: FiniteDuration,
  translator: Translator[F, Text.TypedTag[String]]
) extends Pipe[F, NJEvent, String] with UpdateTranslator[F, Text.TypedTag[String], NJSnsEmail[F]] with all {

  private def copy(
    chunkSize: Int = chunkSize,
    interval: FiniteDuration = interval,
    translator: Translator[F, Text.TypedTag[String]] = translator): NJSnsEmail[F] =
    new NJSnsEmail[F](client, chunkSize, interval, translator)

  def withInterval(fd: FiniteDuration): NJSnsEmail[F] = copy(interval = fd)
  def withChunkSize(cs: Int): NJSnsEmail[F]           = copy(chunkSize = cs)

  override def updateTranslator(
    f: Translator[F, Text.TypedTag[String]] => Translator[F, Text.TypedTag[String]]): NJSnsEmail[F] =
    copy(translator = f(translator))

  override def apply(es: Stream[F, NJEvent]): Stream[F, String] =
    for {
      c <- Stream.resource(client)
      rst <- es
        .evalMap(e =>
          translator.filter {
            case event: ActionEvent => event.actionInfo.nonTrivial
            case _                  => true
          }.translate(e))
        .unNone
        .groupWithin(chunkSize, interval)
        .evalMap { events =>
          val mailBody: String =
            html(body(events.map(hr(_)).toList, footer(hr(p(b("Events/Max: "), s"${events.size}/$chunkSize"))))).render
          c.publish(mailBody).attempt.as(mailBody)
        }
    } yield rst
}
