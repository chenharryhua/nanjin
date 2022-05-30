package com.github.chenharryhua.nanjin.guard.observers

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.amazonaws.services.simpleemail.model.SendEmailResult
import com.amazonaws.services.sns.model.{PublishRequest, PublishResult}
import com.github.chenharryhua.nanjin.aws.*
import com.github.chenharryhua.nanjin.common.{ChunkSize, EmailAddr}
import com.github.chenharryhua.nanjin.common.aws.{EmailContent, SnsArn}
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ServiceStart
import com.github.chenharryhua.nanjin.guard.translators.{Translator, UpdateTranslator}
import eu.timepit.refined.auto.*
import fs2.{Chunk, INothing, Pipe, Stream}
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

import java.util.UUID
import scala.concurrent.duration.{DurationInt, FiniteDuration}

object EmailObserver {

  def apply[F[_]: Async](client: Resource[F, SimpleEmailService[F]]): SesEmailObserver[F] =
    new SesEmailObserver[F](
      client = client,
      chunkSize = ChunkSize(90),
      interval = 180.minutes,
      isNewestFirst = true,
      translator = Translator.html[F])

  def apply[F[_]: Async](client: Resource[F, SimpleNotificationService[F]]): SnsEmailObserver[F] =
    new SnsEmailObserver[F](
      client = client,
      chunkSize = ChunkSize(20),
      interval = 120.minutes,
      isNewestFirst = true,
      translator = Translator.html[F])

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

  private def publish(
    events: Chunk[Text.TypedTag[String]],
    ses: SimpleEmailService[F],
    from: EmailAddr,
    to: NonEmptyList[EmailAddr],
    subject: String): F[Either[Throwable, SendEmailResult]] = {
    val text: List[Text.TypedTag[String]] =
      if (isNewestFirst) events.map(hr(_)).toList.reverse else events.map(hr(_)).toList
    val content = html(body(text, footer(hr(p(b("Events/Max: "), s"${events.size}/$chunkSize"))))).render
    ses.send(EmailContent(from.value, to.map(_.value), subject, content)).attempt
  }

  def observe(from: EmailAddr, to: NonEmptyList[EmailAddr], subject: String): Pipe[F, NJEvent, INothing] = {
    (es: Stream[F, NJEvent]) =>
      val compu = for {
        ses <- Stream.resource(client)
        ofm <- Stream.eval(
          F.ref[Map[UUID, ServiceStart]](Map.empty).map(new FinalizeMonitor(translator.translate, _)))
        _ <- es
          .evalTap(ofm.monitoring)
          .evalMap(translator.translate)
          .unNone
          .groupWithin(chunkSize.value, interval)
          .evalTap(publish(_, ses, from, to, subject).void)
          .onFinalizeCase(
            ofm.terminated(_).flatMap(publish(_, ses, from, to, "Service Termination Notice").void))
      } yield ()
      compu.drain
  }
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

  private def publish(
    events: Chunk[Text.TypedTag[String]],
    sns: SimpleNotificationService[F],
    snsArn: SnsArn,
    subject: String): F[Either[Throwable, PublishResult]] = {
    val text: List[Text.TypedTag[String]] =
      if (isNewestFirst) events.map(hr(_)).toList.reverse else events.map(hr(_)).toList
    val content = html(body(text, footer(hr(p(b("Events/Max: "), s"${events.size}/$chunkSize"))))).render
    val req: PublishRequest = new PublishRequest(snsArn.value, content, subject)
    sns.publish(req).attempt
  }

  def observe(snsArn: SnsArn, subject: String): Pipe[F, NJEvent, INothing] = (es: Stream[F, NJEvent]) =>
    Stream
      .resource(client)
      .flatMap(sns =>
        Stream
          .eval(F.ref[Map[UUID, ServiceStart]](Map.empty).map(new FinalizeMonitor(translator.translate, _)))
          .flatMap(ofm =>
            es.evalTap(ofm.monitoring)
              .evalMap(translator.translate)
              .unNone
              .groupWithin(chunkSize.value, interval)
              .evalTap(publish(_, sns, snsArn, subject).void)
              .onFinalizeCase(
                ofm.terminated(_).flatMap(publish(_, sns, snsArn, "Service Termination Notice").void)))
          .drain)

}
