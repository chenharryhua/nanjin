package com.github.chenharryhua.nanjin.guard.observers.ses

import cats.Endo
import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Ref, Resource}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.*
import com.github.chenharryhua.nanjin.common.aws.EmailContent
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick}
import com.github.chenharryhua.nanjin.common.{ChunkSize, EmailAddr}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.{NJEvent, ServiceStopCause}
import com.github.chenharryhua.nanjin.guard.translator.{ColorScheme, Translator, UpdateTranslator}
import fs2.{Chunk, Pipe, Pull, Stream}
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*
import squants.information.{Bytes, Information, Megabytes}

import java.time.ZoneId
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

sealed trait EmailObserver[F[_]] extends UpdateTranslator[F, Text.TypedTag[String], EmailObserver[F]] {

  def withOldestFirst: EmailObserver[F]
  def observe(from: EmailAddr, to: NonEmptyList[EmailAddr], subject: String): Pipe[F, NJEvent, NJEvent]

  // non public

  private def compose_letter(eventTags: Chunk[ColoredTag], isNewestFirst: Boolean): Letter = {
    val (warns, errors) = eventTags.foldLeft((0, 0)) { case ((w, e), i) =>
      i.color match {
        case ColorScheme.GoodColor  => (w, e)
        case ColorScheme.InfoColor  => (w, e)
        case ColorScheme.WarnColor  => (w + 1, e)
        case ColorScheme.ErrorColor => (w, e + 1)
      }
    }

    val notice: Text.TypedTag[String] =
      if ((warns + errors) > 0) h2(style := "color:red")(s"Pay Attention - $errors Errors, $warns Warnings")
      else h2("All Good")

    val content: List[Text.TypedTag[String]] = {
      val lst = eventTags.map(tag => hr(tag.tag)).toList
      if (isNewestFirst) lst.reverse else lst
    }

    Letter(warns, errors, notice, content)
  }

  final protected def publish_email(
    eventTags: Chunk[ColoredTag],
    ses: SimpleEmailService[F],
    isNewestFirst: Boolean,
    chunkSize: ChunkSize,
    from: EmailAddr,
    to: NonEmptyList[EmailAddr],
    subject: String)(implicit F: Async[F]): F[Unit] = {
    // aws ses maximum message size
    val maximumMessageSize: Information = Megabytes(10)

    val letter = compose_letter(eventTags, isNewestFirst)

    val content: String = letter.emailBody(chunkSize)

    val email: EmailContent =
      if (Bytes(content.length) < maximumMessageSize) {
        EmailContent(from, to, subject, content)
      } else {
        val text = p(
          b(s"Message body size exceeds $maximumMessageSize, which contains ${eventTags.size} events."))
        val msg = html(header, body(letter.notice, text)).render
        EmailContent(from, to, subject, msg)
      }

    ses.send(email).attempt.void
  }

  final protected def send_final_email(
    state: Ref[F, Map[UUID, ServiceStart]],
    cache: Ref[F, Chunk[ColoredTag]])(translate: NJEvent => F[Option[ColoredTag]])(
    send_email: Chunk[ColoredTag] => F[Unit])(implicit F: Async[F]): F[Unit] =
    F.realTimeInstant.flatMap { ts =>
      state.get.flatMap { sm =>
        val stop: F[Chunk[ColoredTag]] =
          Chunk
            .from(sm.values)
            .traverse { ss =>
              translate(
                ServiceStop(
                  ss.serviceParams,
                  ss.serviceParams.toZonedDateTime(ts),
                  ServiceStopCause.ByCancellation))
            }
            .map(_.flattenOption)
        (cache.get, stop).mapN(_ ++ _).flatMap(send_email)
      }
    }
}

object EmailObserver {

  def apply[F[_]: Async](
    client: Resource[F, SimpleEmailService[F]],
    chunkSize: ChunkSize,
    interval: FiniteDuration): EmailObserver[F] =
    new IntervalEmailObserver[F](
      client = client,
      translator = HtmlTranslator[F],
      isNewestFirst = true,
      chunkSize = chunkSize,
      interval = interval)

  def apply[F[_]: Async](
    client: Resource[F, SimpleEmailService[F]],
    chunkSize: ChunkSize,
    policy: Policy,
    zoneId: ZoneId): EmailObserver[F] =
    new PolicyEmailObserver[F](
      client = client,
      translator = HtmlTranslator[F],
      isNewestFirst = true,
      chunkSize = chunkSize,
      policy = policy,
      zoneId = zoneId
    )
}

private class IntervalEmailObserver[F[_]](
  client: Resource[F, SimpleEmailService[F]],
  translator: Translator[F, Text.TypedTag[String]],
  isNewestFirst: Boolean,
  chunkSize: ChunkSize, // number of events in an email
  interval: FiniteDuration
)(implicit F: Async[F])
    extends all with EmailObserver[F] {

  private[this] def copy(
    isNewestFirst: Boolean = isNewestFirst,
    translator: Translator[F, Text.TypedTag[String]] = translator): EmailObserver[F] =
    new IntervalEmailObserver[F](client, translator, isNewestFirst, chunkSize, interval)

  override def withOldestFirst: EmailObserver[F] =
    copy(isNewestFirst = false)

  override def updateTranslator(f: Endo[Translator[F, Text.TypedTag[String]]]): EmailObserver[F] =
    copy(translator = f(translator))

  private def translate(evt: NJEvent): F[Option[ColoredTag]] =
    translator.translate(evt).map(_.map(tag => ColoredTag(tag, ColorScheme.decorate(evt).eval.value)))

  def observe(from: EmailAddr, to: NonEmptyList[EmailAddr], subject: String): Pipe[F, NJEvent, NJEvent] = {
    def send_email(ses: SimpleEmailService[F])(data: Chunk[ColoredTag]): F[Unit] =
      publish_email(data, ses, isNewestFirst, chunkSize, from, to, subject).whenA(data.nonEmpty)

    (events: Stream[F, NJEvent]) =>
      for {
        ses <- Stream.resource(client)
        state <- Stream.eval(F.ref(Map.empty[UUID, ServiceStart]))
        cache <- Stream.eval(F.ref(Chunk.empty[ColoredTag]))
        event <- events.evalTap {
          case ss: ServiceStart => state.update(_.updated(ss.serviceParams.serviceId, ss))
          case ss: ServiceStop  => state.update(_.removed(ss.serviceParams.serviceId))
          case _                => F.unit
        }.observe(
          _.evalMap(translate).unNone
            .evalTap(tag => cache.update(_ ++ Chunk.singleton(tag)))
            .groupWithin(chunkSize.value, interval)
            .evalMap(send_email(ses)(_) *> cache.set(Chunk.empty))
            .drain)
          .onFinalize(send_final_email(state, cache)(translate)(send_email(ses)))
      } yield event
  }
}

private class PolicyEmailObserver[F[_]](
  client: Resource[F, SimpleEmailService[F]],
  translator: Translator[F, Text.TypedTag[String]],
  isNewestFirst: Boolean,
  chunkSize: ChunkSize, // number of events in an email
  policy: Policy,
  zoneId: ZoneId)(implicit F: Async[F])
    extends EmailObserver[F] {

  private[this] def copy(
    isNewestFirst: Boolean = isNewestFirst,
    translator: Translator[F, Text.TypedTag[String]] = translator): EmailObserver[F] =
    new PolicyEmailObserver[F](client, translator, isNewestFirst, chunkSize, policy, zoneId)

  override def withOldestFirst: EmailObserver[F] = copy(isNewestFirst = false)

  override def updateTranslator(f: Endo[Translator[F, Text.TypedTag[String]]]): EmailObserver[F] =
    copy(translator = f(translator))

  private def translate(evt: NJEvent): F[Option[ColoredTag]] =
    translator.translate(evt).map(_.map(tag => ColoredTag(tag, ColorScheme.decorate(evt).eval.value)))

  override def observe(
    from: EmailAddr,
    to: NonEmptyList[EmailAddr],
    subject: String): Pipe[F, NJEvent, NJEvent] = {
    def send_email(ses: SimpleEmailService[F])(data: Chunk[ColoredTag]): F[Unit] =
      publish_email(data, ses, isNewestFirst, chunkSize, from, to, subject).whenA(data.nonEmpty)

    def go(
      ss: Stream[F, Either[NJEvent, Tick]],
      ses: SimpleEmailService[F],
      cache: Ref[F, Chunk[ColoredTag]],
      count: Int): Pull[F, NJEvent, Unit] =
      ss.pull.uncons1.flatMap {
        case Some((head, tail)) =>
          head match {
            case Left(event) =>
              val translated: Pull[F, Nothing, Chunk[ColoredTag]] =
                Pull.eval(translate(event).map(Chunk.fromOption))

              if (count >= chunkSize.value)
                Pull.output1(event) >>
                  Pull.eval(cache.get).flatMap(cc => Pull.eval(send_email(ses)(cc))) >>
                  translated.flatMap(ct => Pull.eval(cache.set(ct))) >>
                  go(tail, ses, cache, 1)
              else
                Pull.output1(event) >>
                  translated.flatMap(ct => Pull.eval(cache.update(_ ++ ct))) >>
                  go(tail, ses, cache, count + 1)

            case Right(_) =>
              Pull.eval(cache.get).flatMap(ck => Pull.eval(send_email(ses)(ck))) >>
                Pull.eval(cache.set(Chunk.empty)) >>
                go(tail, ses, cache, 0)
          }
        case None => Pull.done
      }

    (events: Stream[F, NJEvent]) =>
      for {
        ses <- Stream.resource(client)
        state <- Stream.eval(F.ref(Map.empty[UUID, ServiceStart]))
        cache <- Stream.eval(F.ref(Chunk.empty[ColoredTag]))
        event <- go(
          events.evalTap {
            case ss: ServiceStart => state.update(_.updated(ss.serviceParams.serviceId, ss))
            case ss: ServiceStop  => state.update(_.removed(ss.serviceParams.serviceId))
            case _                => F.unit
          }.map(Left(_)).mergeHaltL(tickStream(policy, zoneId).map(Right(_))),
          ses,
          cache,
          0
        ).stream.onFinalize(send_final_email(state, cache)(translate)(send_email(ses)))

      } yield event
  }
}
