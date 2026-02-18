package com.github.chenharryhua.nanjin.guard.observers.ses

import cats.Endo
import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Ref, Resource}
import cats.syntax.applicative.catsSyntaxApplicativeByName
import cats.syntax.applicativeError.catsSyntaxApplicativeError
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import com.github.chenharryhua.nanjin.aws.*
import com.github.chenharryhua.nanjin.common.aws.EmailContent
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick}
import com.github.chenharryhua.nanjin.common.{ChunkSize, EmailAddr}
import com.github.chenharryhua.nanjin.guard.event.Event.{ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.{Event, ServiceStopCause}
import com.github.chenharryhua.nanjin.guard.translator.{ColorScheme, Translator, UpdateTranslator}
import eu.timepit.refined.auto.*
import fs2.{Chunk, Pipe, Pull, Stream}
import scalatags.Text
import scalatags.Text.all.*
import squants.information.{Bytes, Information, Megabytes}

import java.time.ZoneId
import scala.concurrent.duration.DurationInt
import com.github.chenharryhua.nanjin.guard.config.ServiceId
import com.github.chenharryhua.nanjin.guard.event.Timestamp

object EmailObserver {

  /** @param client
    *   Simple Email Service Client
    */
  def apply[F[_]: Async](client: Resource[F, SimpleEmailService[F]]): EmailObserver[F] =
    new EmailObserver[F](
      client = client,
      translator = HtmlTranslator[F],
      isNewestFirst = true,
      capacity = 100,
      policy = _.fixedDelay(36500.days), // 100 years
      zoneId = ZoneId.systemDefault()
    )
}

final class EmailObserver[F[_]] private (
  client: Resource[F, SimpleEmailService[F]],
  translator: Translator[F, Text.TypedTag[String]],
  isNewestFirst: Boolean,
  capacity: ChunkSize,
  policy: Policy.type => Policy,
  zoneId: ZoneId)(implicit F: Async[F])
    extends UpdateTranslator[F, Text.TypedTag[String], EmailObserver[F]] {

  private[this] def copy(
    isNewestFirst: Boolean = this.isNewestFirst,
    capacity: ChunkSize = this.capacity,
    policy: Policy.type => Policy = this.policy,
    zoneId: ZoneId = this.zoneId,
    translator: Translator[F, Text.TypedTag[String]] = this.translator): EmailObserver[F] =
    new EmailObserver[F](client, translator, isNewestFirst, capacity, policy, zoneId)

  override def updateTranslator(f: Endo[Translator[F, Text.TypedTag[String]]]): EmailObserver[F] =
    copy(translator = f(translator))

  def withOldestFirst: EmailObserver[F] = copy(isNewestFirst = false)
  def withCapacity(cs: ChunkSize): EmailObserver[F] = copy(capacity = cs)
  def withPolicy(f: Policy.type => Policy): EmailObserver[F] = copy(policy = f)
  def withZoneId(zoneId: ZoneId): EmailObserver[F] = copy(zoneId = zoneId)

  private def translate(evt: Event): F[Option[ColoredTag]] =
    translator.translate(evt).map(_.map(tag => ColoredTag(tag, ColorScheme.decorate(evt).eval.value)))

  private def compose_letter(tags: Chunk[ColoredTag]): Letter = {
    val (warns, errors) = tags.foldLeft((0, 0)) { case ((w, e), i) =>
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
      val lst = tags.map(tag => hr(tag.tag)).toList
      if (isNewestFirst) lst.reverse else lst
    }

    Letter(warns, errors, notice, content)
  }

  private def publish_one_email(
    ses: SimpleEmailService[F],
    from: EmailAddr,
    to: NonEmptyList[EmailAddr],
    subject: String)(data: Chunk[ColoredTag]): F[Unit] = {
    // aws ses maximum message size
    val maximumMessageSize: Information = Megabytes(10)

    val letter = compose_letter(data)

    val content: String = letter.emailBody(capacity)

    val email: EmailContent =
      if (Bytes(content.length) < maximumMessageSize) {
        EmailContent(from, to, subject, content)
      } else {
        val text =
          p(b(
            s"Message body size exceeds ${maximumMessageSize.toString()}, which contains ${data.size} events."))
        val msg = html(header, body(letter.notice, text)).render
        EmailContent(from, to, subject, msg)
      }

    ses.send(email).attempt.whenA(data.nonEmpty)
  }

  private def good_bye(
    state: Ref[F, Map[ServiceId, ServiceStart]],
    cache: Ref[F, Chunk[ColoredTag]]): F[Chunk[ColoredTag]] =
    F.realTimeInstant.flatMap { ts =>
      state.get.flatMap { sm =>
        val stop: F[Chunk[ColoredTag]] =
          Chunk.from(sm.values).traverseFilter { ss =>
            translate(
              ServiceStop(
                ss.serviceParams,
                Timestamp(ss.serviceParams.toZonedDateTime(ts)),
                ServiceStopCause.ByCancellation))
          }
        (cache.get, stop).mapN(_ ++ _)
      }
    }

  def observe(from: EmailAddr, to: NonEmptyList[EmailAddr], subject: String): Pipe[F, Event, Event] = {

    def go(
      ss: Stream[F, Either[Event, Tick]],
      send_email: Chunk[ColoredTag] => F[Unit],
      cache: Ref[F, Chunk[ColoredTag]]): Pull[F, Event, Unit] =
      ss.pull.uncons1.flatMap {
        case Some((head, tail)) =>
          head match {
            case Left(event) =>
              val send_and_update: F[Unit] = translate(event).flatMap {
                case Some(ct) =>
                  cache.flatModify { tags =>
                    if (tags.size < capacity.value)
                      (tags ++ Chunk.singleton(ct)) -> F.unit
                    else
                      Chunk.singleton(ct) -> send_email(tags)
                  }
                case None => F.unit
              }

              Pull.output1[F, Event](event) >>
                Pull.eval(send_and_update) >>
                go(tail, send_email, cache)

            case Right(_) => // tick
              Pull.eval(cache.flatModify(tags => Chunk.empty -> send_email(tags))) >>
                go(tail, send_email, cache)
          }
        case None => Pull.done // leave cache to be handled by finalizer
      }

    (events: Stream[F, Event]) =>
      for {
        ses <- Stream.resource(client)
        state <- Stream.eval(F.ref(Map.empty[ServiceId, ServiceStart]))
        cache <- Stream.eval(F.ref(Chunk.empty[ColoredTag]))
        monitor = events.evalTap {
          case ss: ServiceStart => state.update(_.updated(ss.serviceParams.serviceId, ss))
          case ss: ServiceStop  => state.update(_.removed(ss.serviceParams.serviceId))
          case _                => F.unit
        }.map(Left(_))
        ticks = tickStream.tickScheduled[F](zoneId, policy).map(Right(_))
        send_email = publish_one_email(ses, from, to, subject)(_)
        event <- go(monitor.mergeHaltBoth(ticks), send_email, cache).stream
          .onFinalize(good_bye(state, cache).flatMap(send_email))
      } yield event
  }
}
