package com.github.chenharryhua.nanjin.guard.observers.ses

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Ref, Resource}
import cats.syntax.applicativeError.given
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.{Endo, Eval}
import com.github.chenharryhua.nanjin.aws.*
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick}
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.config.{Capacity, ServiceId}
import com.github.chenharryhua.nanjin.guard.event.Event.{ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.{Event, StopReason}
import com.github.chenharryhua.nanjin.guard.translator.{eventLogLevel, Translator}
import fs2.{Chunk, Pipe, Pull, Stream}
import scalatags.Text
import scalatags.Text.all.*
import squants.information.{Bytes, Information, Megabytes}

import java.time.ZoneId

/** Observer that batches events into HTML emails and delivers them via AWS SES.
  *
  * Obtain one with `EmailObserver.apply` and adjust it with the `with*` methods; each returns a new observer
  * so configuration composes fluently. Wire it into a service with `observe`.
  *
  * Translated events are buffered and flushed as a single email on three occasions: when the buffer reaches
  * `capacity`, on each scheduled tick of `policy`, and once more on stream finalization (carrying any
  * remaining buffered events plus a synthesized `ServiceStop` for each service still running). An empty flush
  * is sent as a heartbeat, confirming the observer is alive even when there is nothing to report.
  *
  * The observer's lifetime tracks the incoming event stream: it runs until that stream ends, at which point
  * the finalizer flush fires. Exhausting `policy` does not stop the observer; it only stops the scheduled
  * flushes. The default `policy` is empty (no ticks), so out of the box emails are emitted purely on
  * `capacity` and on finalization.
  */
sealed trait EmailObserver[F[_]] {

  /** Order the email body oldest-first instead of the default newest-first. */
  def withOldestFirst: EmailObserver[F]

  /** Set the maximum number of buffered events before a flush.
    *
    * Values below 5 are raised to 5, so the buffer always holds at least a few events.
    *
    * @param num
    *   desired buffer capacity; clamped to a minimum of 5.
    */
  def withCapacity(num: Int): EmailObserver[F]

  /** Set the schedule on which buffered events are flushed. */
  def withPolicy(f: Policy.type => Policy): EmailObserver[F]

  /** Set the time zone used to interpret the flush schedule. */
  def withZoneId(zoneId: ZoneId): EmailObserver[F]

  /** Transform the event-to-HTML translator, e.g. to skip certain event kinds. */
  def withTranslator(f: Endo[Translator[F, Text.TypedTag[String]]]): EmailObserver[F]

  /** Build a pipe that observes events, batches them into HTML emails, and sends them via SES.
    *
    * Events pass through unchanged (the pipe is a side-effecting tap). Emails are flushed on capacity, on
    * each scheduled tick, and on finalization; an empty flush is sent as a heartbeat. The pipe runs until the
    * incoming event stream ends; an exhausted `policy` only stops the scheduled flushes.
    *
    * @param from
    *   the sender address.
    * @param to
    *   the recipient addresses.
    * @param subject
    *   the email subject line, applied to every email.
    */
  def observe(from: Email, to: NonEmptyList[Email], subject: String): Pipe[F, Event, Event]
}

object EmailObserver {

  /** Create an `EmailObserver` with default configuration: HTML translator, newest-first ordering, capacity
    * 100, an empty flush schedule (flushing then relies on capacity and finalization), and the system time
    * zone.
    */
  def apply[F[_]: Async](client: Resource[F, SimpleEmailService[F]]): EmailObserver[F] =
    new EmailObserverImpl[F](
      Params(
        client = client,
        translator = HtmlTranslator[F],
        isNewestFirst = true,
        capacity = Capacity(100),
        policy = _.empty,
        zoneId = ZoneId.systemDefault())
    )
}

final private class EmailObserverImpl[F[_]](params: Params[F])(using F: Async[F]) extends EmailObserver[F] {
  private def copy(p: Params[F]): EmailObserver[F] =
    new EmailObserverImpl[F](p)

  override def withOldestFirst: EmailObserver[F] = copy(params.copy(isNewestFirst = false))

  override def withCapacity(num: Int): EmailObserver[F] = copy(params.copy(capacity = Capacity(num.max(5))))

  override def withPolicy(f: Policy.type => Policy): EmailObserver[F] = copy(params.copy(policy = f))

  override def withZoneId(zoneId: ZoneId): EmailObserver[F] = copy(params.copy(zoneId = zoneId))

  override def withTranslator(f: Endo[Translator[F, Text.TypedTag[String]]]): EmailObserver[F] =
    copy(params.copy(translator = f(params.translator)))

  /*
   * pipe
   */

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
                None,
                ss.brief,
                ss.serviceIdentity.toTimestamp(ts),
                StopReason.ByCancellation))
          }
        (cache.get, stop).mapN(_ ++ _)
      }
    }

  override def observe(from: Email, to: NonEmptyList[Email], subject: String): Pipe[F, Event, Event] = {

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
        // mergeHaltL: the observer's lifetime tracks the event stream, not the tick policy. When the policy
        // is exhausted the ticks stream ends, but the merge keeps running off events; scheduled flushes
        // simply stop while capacity flushes and the finalizer flush continue.
        event <- go(monitor.mergeHaltL(ticks), send_email, cache)
          .stream
          .onFinalize(good_bye(state, cache).flatMap(send_email))
      } yield event
  }
}
