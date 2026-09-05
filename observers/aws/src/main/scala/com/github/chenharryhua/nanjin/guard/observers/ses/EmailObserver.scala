package com.github.chenharryhua.nanjin.guard.observers.ses

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Ref, Resource}
import cats.syntax.applicativeError.given
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.{Applicative, Endo, Eval}
import com.github.chenharryhua.nanjin.aws.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick}
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.config.ServiceId
import com.github.chenharryhua.nanjin.guard.event.Event.{ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.{Event, StopReason}
import com.github.chenharryhua.nanjin.guard.translator.{eventLogLevel, Translator}
import fs2.{Chunk, Pipe, Pull, Stream}
import scalatags.Text
import scalatags.Text.all.*
import squants.information.{Bytes, Information, Megabytes}

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

/** Observer that batches events into HTML emails and delivers them via AWS SES.
  *
  * Translated events are buffered and flushed as a single email on three occasions: when the buffer reaches
  * `capacity`, on each scheduled tick of `policy`, and once more on stream finalization (carrying any
  * remaining buffered events plus a synthesized `ServiceStop` for each service still running). An empty flush
  * is sent as a heartbeat, confirming the observer is alive even when there is nothing to report.
  */
object EmailObserver {

  /** Immutable configuration for an [[EmailObserver]]. Build one with [[Params.apply]] and adjust it with the
    * `with*` methods, then hand it to [[EmailObserver.apply]].
    *
    * @param client
    *   resource yielding the SES client used to send.
    * @param translator
    *   renders each event into an HTML fragment; events the translator drops are not included.
    * @param isNewestFirst
    *   when `true`, the most recent event appears at the top of the email body.
    * @param capacity
    *   maximum number of events buffered before an email is flushed.
    * @param policy
    *   schedule on which buffered events are flushed; the default never fires, so flushing relies on
    *   `capacity` and finalization.
    * @param zoneId
    *   time zone used to interpret the flush schedule.
    */
  final case class Params[F[_]](
    client: Resource[F, SimpleEmailService[F]],
    translator: Translator[F, Text.TypedTag[String]],
    isNewestFirst: Boolean,
    capacity: ChunkSize,
    policy: Policy.type => Policy,
    zoneId: ZoneId) {

    /** Order the email body oldest-first instead of the default newest-first. */
    def withOldestFirst: Params[F] = copy(isNewestFirst = false)

    /** Set the maximum number of buffered events before a flush. */
    def withCapacity(cs: ChunkSize): Params[F] = copy(capacity = cs)

    /** Set the schedule on which buffered events are flushed. */
    def withPolicy(f: Policy.type => Policy): Params[F] = copy(policy = f)

    /** Set the time zone used to interpret the flush schedule. */
    def withZoneId(zoneId: ZoneId): Params[F] = copy(zoneId = zoneId)

    /** Transform the event-to-HTML translator, e.g. to skip certain event kinds. */
    def withTranslator(f: Endo[Translator[F, Text.TypedTag[String]]]): Params[F] =
      copy(translator = f(translator))
  }

  object Params {

    /** Default configuration for `client`: HTML translator, newest-first, capacity 100, a schedule that
      * effectively never fires (so flushing relies on capacity and finalization), and the system time zone.
      */
    def apply[F[_]: Applicative](client: Resource[F, SimpleEmailService[F]]): Params[F] =
      Params(
        client = client,
        translator = HtmlTranslator[F],
        isNewestFirst = true,
        capacity = ChunkSize(100),
        policy = _.fixedDelay(36500.days),
        zoneId = ZoneId.systemDefault())
  }

  /** Build an observer from a fully configured [[Params]]. */
  def apply[F[_]: Async](params: Params[F]): EmailObserver[F] =
    new EmailObserver[F](params)
}

final class EmailObserver[F[_]] private (params: EmailObserver.Params[F])(using F: Async[F]) {
  private def translate(evt: Event): F[Option[ColoredTag]] =
    params.translator
      .translate(evt)
      .map(_.map(tag => ColoredTag(tag, eventLogLevel[Eval, LogLevel](evt).eval.value)))

  private def compose_letter(tags: Chunk[ColoredTag]): Letter = {
    val (warns, errors) = tags.foldLeft((0, 0)) { case ((w, e), i) =>
      i.color match {
        case LogLevel.Good  => (w, e)
        case LogLevel.Info  => (w, e)
        case LogLevel.Debug => (w, e)
        case LogLevel.Warn  => (w + 1, e)
        case LogLevel.Error => (w, e + 1)
      }
    }

    val notice: Text.TypedTag[String] =
      if ((warns + errors) > 0) h2(style := "color:red")(s"Pay Attention - $errors Errors, $warns Warnings")
      else h2("All Good")

    val content: List[Text.TypedTag[String]] = {
      val lst = tags.map(tag => hr(tag.tag)).toList
      if (params.isNewestFirst) lst.reverse else lst
    }

    Letter(warns, errors, notice, content)
  }

  private def publish_one_email(
    ses: SimpleEmailService[F],
    from: Email,
    to: NonEmptyList[Email],
    subject: String)(data: Chunk[ColoredTag]): F[Unit] = {
    // aws ses maximum message size
    val maximumMessageSize: Information = Megabytes(10)

    val letter = compose_letter(data)

    val content: String = letter.emailBody(params.capacity)

    val email: EmailContent =
      if (Bytes(content.length) < maximumMessageSize) {
        EmailContent(from, to, subject, content)
      } else {
        val text =
          p(b(s"Message body size exceeds ${maximumMessageSize.value}, which contains ${data.size} events."))
        val msg = html(header, body(letter.notice, text)).render
        EmailContent(from, to, subject, msg)
      }

    // Always send, even when data is empty: an empty email is a heartbeat signalling the service is still
    // running. Send failures are already logged by SimpleEmailService; attempt swallows them so one failed
    // email does not tear down the observer.
    ses.send(email).attempt.void
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
                ss.serviceIdentity,
                ss.policy,
                ss.brief,
                ss.serviceIdentity.toTimestamp(ts),
                StopReason.ByCancellation))
          }
        (cache.get, stop).mapN(_ ++ _)
      }
    }

  /** Build a pipe that observes events, batches them into HTML emails, and sends them via SES.
    *
    * Events pass through unchanged (the pipe is a side-effecting tap). Emails are flushed on capacity, on
    * each scheduled tick, and on finalization; an empty flush is sent as a heartbeat.
    *
    * @param from
    *   the sender address.
    * @param to
    *   the recipient addresses.
    * @param subject
    *   the email subject line, applied to every email.
    */
  def observe(from: Email, to: NonEmptyList[Email], subject: String): Pipe[F, Event, Event] = {

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
                    if (tags.size < params.capacity.value)
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
        ses <- Stream.resource(params.client)
        state <- Stream.eval(F.ref(Map.empty[ServiceId, ServiceStart]))
        cache <- Stream.eval(F.ref(Chunk.empty[ColoredTag]))
        monitor = events.evalTap {
          case ss: ServiceStart => state.update(_.updated(ss.serviceIdentity.serviceId, ss))
          case ss: ServiceStop  => state.update(_.removed(ss.serviceIdentity.serviceId))
          case _                => F.unit
        }.map(Left(_))
        ticks = tickStream.tickScheduled[F](params.zoneId, params.policy).map(Right(_))
        send_email = publish_one_email(ses, from, to, subject)(_)
        event <- go(monitor.mergeHaltBoth(ticks), send_email, cache).stream
          .onFinalize(good_bye(state, cache).flatMap(send_email))
      } yield event
  }
}
