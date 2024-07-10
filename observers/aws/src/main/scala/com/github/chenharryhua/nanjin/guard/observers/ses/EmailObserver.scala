package com.github.chenharryhua.nanjin.guard.observers.ses

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Clock, Ref, Resource}
import cats.syntax.all.*
import cats.{Endo, Monad}
import com.github.chenharryhua.nanjin.aws.*
import com.github.chenharryhua.nanjin.common.aws.EmailContent
import com.github.chenharryhua.nanjin.common.{ChunkSize, EmailAddr}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, ServiceStopCause}
import com.github.chenharryhua.nanjin.guard.translator.{ColorScheme, Translator, UpdateTranslator}
import fs2.{Chunk, Pipe, Stream}
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*
import squants.information.{Bytes, Information, Megabytes}

import java.util.UUID
import scala.concurrent.duration.{DurationInt, FiniteDuration}

object EmailObserver {

  def apply[F[_]: Async](client: Resource[F, SimpleEmailService[F]]): EmailObserver[F] =
    new EmailObserver[F](
      client = client,
      chunkSize = ChunkSize(180),
      interval = 12.hours,
      isNewestFirst = true,
      translator = HtmlTranslator[F])
}

final class EmailObserver[F[_]] private (
  client: Resource[F, SimpleEmailService[F]],
  chunkSize: ChunkSize, // number of events in an email
  interval: FiniteDuration, // send out email every interval
  isNewestFirst: Boolean, // the latest event comes first
  translator: Translator[F, Text.TypedTag[String]])(implicit F: Async[F])
    extends UpdateTranslator[F, Text.TypedTag[String], EmailObserver[F]] with all {

  private[this] def copy(
    chunkSize: ChunkSize = chunkSize,
    interval: FiniteDuration = interval,
    isNewestFirst: Boolean = isNewestFirst,
    translator: Translator[F, Text.TypedTag[String]] = translator): EmailObserver[F] =
    new EmailObserver[F](client, chunkSize, interval, isNewestFirst, translator)

  def withInterval(fd: FiniteDuration): EmailObserver[F] = copy(interval = fd)

  def withChunkSize(cs: ChunkSize): EmailObserver[F] = copy(chunkSize = cs)

  def withOldestFirst: EmailObserver[F] = copy(isNewestFirst = false)

  override def updateTranslator(f: Endo[Translator[F, Text.TypedTag[String]]]): EmailObserver[F] =
    copy(translator = f(translator))

  // aws ses maximum message size
  private val maximumMessageSize: Information = Megabytes(10)

  private def publish(
    eventTags: Chunk[(Text.TypedTag[String], ColorScheme)],
    ses: SimpleEmailService[F],
    from: EmailAddr,
    to: NonEmptyList[EmailAddr],
    subject: String): F[Unit] = {
    val (warns, errors) = eventTags.foldLeft((0, 0)) { case ((w, e), i) =>
      i._2 match {
        case ColorScheme.GoodColor  => (w, e)
        case ColorScheme.InfoColor  => (w, e)
        case ColorScheme.WarnColor  => (w + 1, e)
        case ColorScheme.ErrorColor => (w, e + 1)
      }
    }

    val header: Text.TypedTag[String] = head(tag("style")("""
        td, th {text-align: left; padding: 2px; border: 1px solid;}
        table {
          border-collapse: collapse;
          width: 90%;
        }
      """))

    val notice: Text.TypedTag[String] =
      if ((warns + errors) > 0) h2(style := "color:red")(s"Pay Attention - $errors Errors, $warns Warnings")
      else h2("All Good")

    val text: List[Text.TypedTag[String]] =
      if (isNewestFirst) eventTags.map(tag => hr(tag._1)).toList.reverse
      else eventTags.map(tag => hr(tag._1)).toList

    val content: String = html(
      header,
      body(notice, text, footer(hr(p(b("Events/Max: "), s"${eventTags.size}/$chunkSize"))))).render

    if (Bytes(content.length) < maximumMessageSize) {
      ses.send(EmailContent(from, to, subject, content)).attempt.void
    } else {
      val text = p(
        b(s"Message body size exceeds $maximumMessageSize, which contains ${eventTags.size} events."))
      val msg = html(header, body(notice, text)).render
      ses.send(EmailContent(from, to, subject, msg)).attempt.void
    }
  }

  private def translate(evt: NJEvent): F[Option[(Text.TypedTag[String], ColorScheme)]] =
    translator.translate(evt).map(_.map(tags => (tags, ColorScheme.decorate(evt).eval.value)))

  def observe(from: EmailAddr, to: NonEmptyList[EmailAddr], subject: String): Pipe[F, NJEvent, NJEvent] = {
    (events: Stream[F, NJEvent]) =>
      for {
        ses <- Stream.resource(client)
        buff <- Stream.eval(F.ref[Vector[NJEvent]](Vector.empty))
        ref <- Stream.eval(F.ref[Map[UUID, ServiceStart]](Map.empty))
        ofm = new EmailFinalizeMonitor(translate, buff, ref)
        event <- events
          .evalTap(ofm.monitoring)
          .observe(
            _.evalMap(translate).unNone
              .groupWithin(chunkSize.value, interval)
              .evalMap(publish(_, ses, from, to, subject) >> ofm.reset)
              .drain)
          .onFinalize(ofm.terminated.flatMap(ca => publish(ca, ses, from, to, subject).whenA(ca.nonEmpty)))
      } yield event
  }
}

final private class EmailFinalizeMonitor[F[_]: Clock: Monad, A](
  translate: NJEvent => F[Option[A]],
  buff: Ref[F, Vector[NJEvent]],
  ref: Ref[F, Map[UUID, ServiceStart]]) {
  def monitoring(event: NJEvent): F[Unit] = (event match {
    case ss: ServiceStart => ref.update(_.updated(ss.serviceParams.serviceId, ss))
    case ss: ServiceStop  => ref.update(_.removed(ss.serviceParams.serviceId))
    case _                => Monad[F].unit
  }) >> buff.update(_.appended(event))

  def reset: F[Unit] = buff.set(Vector.empty)

  val terminated: F[Chunk[A]] = for {
    ts <- Clock[F].realTimeInstant
    stopEvents <- ref.get.map { m =>
      m.values.toVector.map { ss =>
        ServiceStop(ss.serviceParams, ss.serviceParams.toZonedDateTime(ts), ServiceStopCause.ByCancellation)
      }
    }
    msg <- buff.get.flatMap(events => Chunk.from(events ++ stopEvents).traverseFilter(translate))
  } yield msg
}
