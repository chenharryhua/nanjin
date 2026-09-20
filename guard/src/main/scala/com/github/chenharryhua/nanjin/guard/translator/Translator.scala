package com.github.chenharryhua.nanjin.guard.translator

import alleycats.Pure
import cats.data.{Kleisli, OptionT}
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import cats.{Applicative, Endo, Functor, FunctorFilter, Monad, Traverse}
import com.github.chenharryhua.nanjin.guard.event.Event.*
import com.github.chenharryhua.nanjin.guard.event.{Event, EventPipe}

/** A builder-side capability for types that wrap a `Translator` and can be reconfigured by transforming it.
  *
  * `withTranslator` applies an endomorphism to the enclosed `Translator[F, A]` and returns the enclosing type
  * `B` (typically the observer or config being built), so callers can tweak translation without rebuilding
  * the surrounding value.
  *
  * @tparam F
  *   the effect of the underlying translator
  * @tparam A
  *   the translator's output type
  * @tparam B
  *   the enclosing type returned after the update
  */
trait UpdateTranslator[F[_], A, B] {
  def withTranslator(f: Endo[Translator[F, A]]): B
}

/** Maps guard `Event`s to an optional output `A` in effect `F`, with one handler per event type.
  *
  * A `Translator` bundles five `Kleisli[OptionT[F, *], _, A]` handlers, one for each concrete `Event` subtype
  * (`ServiceStart`, `ServicePanic`, `ServiceStop`, `ReportedEvent`, `MetricsSnapshot`). Translating an event
  * dispatches to the matching handler and yields `F[Option[A]]`, where `None` means the event was skipped or
  * filtered out. Observers use this to turn events into their target representation (Slack messages, JSON
  * rows, log lines, etc.).
  *
  * Handlers are set with the `with*` families, cleared with the `skip*` family, gated with `filter`, and
  * composed with `flatMap` (see also the `Monad`/`FunctorFilter` instance in the companion).
  *
  * @tparam F
  *   the effect in which translation runs
  * @tparam A
  *   the translated output type
  */
final case class Translator[F[_], A](
  serviceStart: Kleisli[OptionT[F, *], ServiceStart, A],
  servicePanic: Kleisli[OptionT[F, *], ServicePanic, A],
  serviceStop: Kleisli[OptionT[F, *], ServiceStop, A],
  reportedEvent: Kleisli[OptionT[F, *], ReportedEvent, A],
  metricsSnapshot: Kleisli[OptionT[F, *], MetricsSnapshot, A]
) {

  /** Dispatches an event to the handler for its concrete type and runs it, yielding `None` when that handler
    * declines to produce an output.
    */
  def translate(event: Event): F[Option[A]] = event match {
    case e: ServiceStart    => serviceStart.run(e).value
    case e: ServicePanic    => servicePanic.run(e).value
    case e: ServiceStop     => serviceStop.run(e).value
    case e: MetricsSnapshot => metricsSnapshot.run(e).value
    case e: ReportedEvent   => reportedEvent.run(e).value
  }

  /** Gates every handler by a predicate on the event: events that fail `f` translate to `None` while passing
    * events run their existing handler.
    */
  def filter(f: Event => Boolean)(using F: Applicative[F]): Translator[F, A] =
    Translator[F, A](
      Kleisli(ss => if (f(ss)) serviceStart.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) servicePanic.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) serviceStop.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) reportedEvent.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) metricsSnapshot.run(ss) else OptionT(F.pure(None)))
    )

  /** Gates translation using an `EventPipe`'s own filter; see `filter(f: Event => Boolean)`. */
  def filter(pipe: EventPipe)(using F: Applicative[F]): Translator[F, A] =
    filter(pipe.filter)

  /** Translates every event in a `Traverse`-able container, preserving its shape and yielding an `Option[A]`
    * per element.
    */
  // for convenience
  def traverse[G[_]](ge: G[Event])(using F: Applicative[F], G: Traverse[G]): F[G[Option[A]]] =
    G.traverse[F, Event, Option[A]](ge)(translate)

  /* The skip* methods replace a single handler with a no-op so that event type always translates
   * to None; skipAll replaces every handler at once. */

  /** Ignores `ServiceStart` events (translates them to `None`). */
  def skipServiceStart(using F: Applicative[F]): Translator[F, A] =
    copy(serviceStart = Translator.noop[F, A])

  /** Ignores `ServicePanic` events (translates them to `None`). */
  def skipServicePanic(using F: Applicative[F]): Translator[F, A] =
    copy(servicePanic = Translator.noop[F, A])

  /** Ignores `ServiceStop` events (translates them to `None`). */
  def skipServiceStop(using F: Applicative[F]): Translator[F, A] =
    copy(serviceStop = Translator.noop[F, A])

  /** Ignores `MetricsSnapshot` events (translates them to `None`). */
  def skipMetricsSnapshot(using F: Applicative[F]): Translator[F, A] =
    copy(metricsSnapshot = Translator.noop[F, A])

  /** Ignores `ReportedEvent` events (translates them to `None`). */
  def skipReportedEvent(using F: Applicative[F]): Translator[F, A] =
    copy(reportedEvent = Translator.noop[F, A])

  /** Ignores every event; equivalent to `Translator.empty`. */
  def skipAll(using F: Applicative[F]): Translator[F, A] =
    Translator.empty[F, A]

  /* Each with* family sets the handler for one event type. The four overloads accept, in order, an
   * effectful optional result (F[Option[A]]), a pure optional result (Option[A]), an effectful
   * total result (F[A]), or a pure total result (A); the latter three lift into F[Option[A]] under
   * the stated constraint. */

  /** Sets the `ServiceStart` handler from an effectful, optional result. */
  def withServiceStart(f: ServiceStart => F[Option[A]]): Translator[F, A] =
    copy(serviceStart = Kleisli(a => OptionT(f(a))))

  /** Sets the `ServiceStart` handler from a pure, optional result. */
  def withServiceStart(f: ServiceStart => Option[A])(using F: Applicative[F]): Translator[F, A] =
    copy(serviceStart = Kleisli(a => OptionT(F.pure(f(a)))))

  /** Sets the `ServiceStart` handler from an effectful, total result. */
  def withServiceStart(f: ServiceStart => F[A])(using F: Functor[F]): Translator[F, A] =
    copy(serviceStart = Kleisli(a => OptionT(f(a).map(Some(_)))))

  /** Sets the `ServiceStart` handler from a pure, total result. */
  def withServiceStart(f: ServiceStart => A)(using F: Pure[F]): Translator[F, A] =
    copy(serviceStart = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  /** Sets the `ServicePanic` handler from an effectful, optional result. */
  def withServicePanic(f: ServicePanic => F[Option[A]]): Translator[F, A] =
    copy(servicePanic = Kleisli(a => OptionT(f(a))))

  /** Sets the `ServicePanic` handler from a pure, optional result. */
  def withServicePanic(f: ServicePanic => Option[A])(using F: Applicative[F]): Translator[F, A] =
    copy(servicePanic = Kleisli(a => OptionT(F.pure(f(a)))))

  /** Sets the `ServicePanic` handler from an effectful, total result. */
  def withServicePanic(f: ServicePanic => F[A])(using F: Functor[F]): Translator[F, A] =
    copy(servicePanic = Kleisli(a => OptionT(f(a).map(Some(_)))))

  /** Sets the `ServicePanic` handler from a pure, total result. */
  def withServicePanic(f: ServicePanic => A)(using F: Pure[F]): Translator[F, A] =
    copy(servicePanic = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  /** Sets the `ServiceStop` handler from an effectful, optional result. */
  def withServiceStop(f: ServiceStop => F[Option[A]]): Translator[F, A] =
    copy(serviceStop = Kleisli(a => OptionT(f(a))))

  /** Sets the `ServiceStop` handler from a pure, optional result. */
  def withServiceStop(f: ServiceStop => Option[A])(using F: Applicative[F]): Translator[F, A] =
    copy(serviceStop = Kleisli(a => OptionT(F.pure(f(a)))))

  /** Sets the `ServiceStop` handler from an effectful, total result. */
  def withServiceStop(f: ServiceStop => F[A])(using F: Functor[F]): Translator[F, A] =
    copy(serviceStop = Kleisli(a => OptionT(f(a).map(Some(_)))))

  /** Sets the `ServiceStop` handler from a pure, total result. */
  def withServiceStop(f: ServiceStop => A)(using F: Pure[F]): Translator[F, A] =
    copy(serviceStop = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  /** Sets the `MetricsSnapshot` handler from an effectful, optional result. */
  def withMetricsSnapshot(f: MetricsSnapshot => F[Option[A]]): Translator[F, A] =
    copy(metricsSnapshot = Kleisli(a => OptionT(f(a))))

  /** Sets the `MetricsSnapshot` handler from a pure, optional result. */
  def withMetricsSnapshot(f: MetricsSnapshot => Option[A])(using F: Applicative[F]): Translator[F, A] =
    copy(metricsSnapshot = Kleisli(a => OptionT(F.pure(f(a)))))

  /** Sets the `MetricsSnapshot` handler from an effectful, total result. */
  def withMetricsSnapshot(f: MetricsSnapshot => F[A])(using F: Functor[F]): Translator[F, A] =
    copy(metricsSnapshot = Kleisli(a => OptionT(f(a).map(Some(_)))))

  /** Sets the `MetricsSnapshot` handler from a pure, total result. */
  def withMetricsSnapshot(f: MetricsSnapshot => A)(using F: Pure[F]): Translator[F, A] =
    copy(metricsSnapshot = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  /** Sets the `ReportedEvent` handler from an effectful, optional result. */
  def withReportedEvent(f: ReportedEvent => F[Option[A]]): Translator[F, A] =
    copy(reportedEvent = Kleisli(a => OptionT(f(a))))

  /** Sets the `ReportedEvent` handler from a pure, optional result. */
  def withReportedEvent(f: ReportedEvent => Option[A])(using F: Applicative[F]): Translator[F, A] =
    copy(reportedEvent = Kleisli(a => OptionT(F.pure(f(a)))))

  /** Sets the `ReportedEvent` handler from an effectful, total result. */
  def withReportedEvent(f: ReportedEvent => F[A])(using F: Functor[F]): Translator[F, A] =
    copy(reportedEvent = Kleisli(a => OptionT(f(a).map(Some(_)))))

  /** Sets the `ReportedEvent` handler from a pure, total result. */
  def withReportedEvent(f: ReportedEvent => A)(using F: Pure[F]): Translator[F, A] =
    copy(reportedEvent = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  /** Sequences this translator with a second one selected from its output: translate the event here, and if
    * it produced an `A`, run `f(a)`'s translator on the same event. Events this translator skips stay
    * skipped.
    */
  def flatMap[B](f: A => Translator[F, B])(using F: Monad[F]): Translator[F, B] = {
    val go: Event => F[Option[Translator[F, B]]] = { (evt: Event) => translate(evt).map(_.map(f)) }
    Translator
      .empty[F, B]
      .withServiceStart(evt => go(evt).flatMap(_.flatTraverse(_.serviceStart.run(evt).value)))
      .withServicePanic(evt => go(evt).flatMap(_.flatTraverse(_.servicePanic.run(evt).value)))
      .withServiceStop(evt => go(evt).flatMap(_.flatTraverse(_.serviceStop.run(evt).value)))
      .withReportedEvent(evt => go(evt).flatMap(_.flatTraverse(_.reportedEvent.run(evt).value)))
      .withMetricsSnapshot(evt => go(evt).flatMap(_.flatTraverse(_.metricsSnapshot.run(evt).value)))
  }
}

object Translator {

  /** The combined `Monad` and `FunctorFilter` capability provided for `Translator[F, *]` at a fixed effect
    * `F`.
    */
  private type MF[F[_]] = Monad[[A] =>> Translator[F, A]] & FunctorFilter[[A] =>> Translator[F, A]]

  /** `Monad` + `FunctorFilter` instance for `Translator[F, *]`: `pure` builds a translator whose every
    * handler returns the given constant, `flatMap`/`tailRecM` chain translators over the same event, and
    * `mapFilter` drops outputs mapped to `None`.
    */
  given monadTranslator[F[_]](using F: Monad[F]): MF[F] =
    new Monad[Translator[F, *]] with FunctorFilter[Translator[F, *]] {
      override def flatMap[A, B](fa: Translator[F, A])(f: A => Translator[F, B]): Translator[F, B] =
        fa.flatMap(f)

      override def tailRecM[A, B](a: A)(f: A => Translator[F, Either[A, B]]): Translator[F, B] = {
        def mapper(oeab: Option[Either[A, B]]): Either[A, Option[B]] =
          oeab match {
            case None           => Right(None)
            case Some(Right(r)) => Right(Some(r))
            case Some(Left(l))  => Left(l)
          }

        val serviceStart: Kleisli[OptionT[F, *], ServiceStart, B] =
          Kleisli((ss: ServiceStart) =>
            OptionT(F.tailRecM(a)(x => f(x).serviceStart.run(ss).value.map(mapper))))

        val servicePanic: Kleisli[OptionT[F, *], ServicePanic, B] =
          Kleisli((ss: ServicePanic) =>
            OptionT(F.tailRecM(a)(x => f(x).servicePanic.run(ss).value.map(mapper))))

        val serviceStop: Kleisli[OptionT[F, *], ServiceStop, B] =
          Kleisli((ss: ServiceStop) =>
            OptionT(F.tailRecM(a)(x => f(x).serviceStop.run(ss).value.map(mapper))))

        val metricsSnapshot: Kleisli[OptionT[F, *], MetricsSnapshot, B] =
          Kleisli((ss: MetricsSnapshot) =>
            OptionT(F.tailRecM(a)(x => f(x).metricsSnapshot.run(ss).value.map(mapper))))

        val reportedEvent: Kleisli[OptionT[F, *], ReportedEvent, B] =
          Kleisli((ss: ReportedEvent) =>
            OptionT(F.tailRecM(a)(x => f(x).reportedEvent.run(ss).value.map(mapper))))

        Translator[F, B](
          serviceStart,
          servicePanic,
          serviceStop,
          reportedEvent,
          metricsSnapshot
        )
      }

      override def pure[A](x: A): Translator[F, A] =
        Translator[F, A](
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x)))),
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x)))),
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x)))),
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x)))),
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x))))
        )

      override val functor: Functor[Translator[F, *]] = this

      override def mapFilter[A, B](fa: Translator[F, A])(f: A => Option[B]): Translator[F, B] = {
        def go(e: Event): F[Option[B]] = fa.translate(e).map(_.flatMap(f))
        Translator
          .empty[F, B]
          .withServiceStart(go)
          .withServicePanic(go)
          .withServiceStop(go)
          .withReportedEvent(go)
          .withMetricsSnapshot(go)
      }
    }

  /** A handler that always yields `None`, used to skip an event type. */
  def noop[F[_], A](using F: Applicative[F]): Kleisli[OptionT[F, *], Event, A] =
    Kleisli(_ => OptionT(F.pure(None)))

  /** A translator that skips every event (all handlers are `noop`). */
  def empty[F[_]: Applicative, A]: Translator[F, A] =
    Translator[F, A](
      noop[F, A],
      noop[F, A],
      noop[F, A],
      noop[F, A],
      noop[F, A]
    )

  /** The identity translator: every event is translated to itself. */
  def idTranslator[F[_]](using F: Applicative[F]): Translator[F, Event] =
    Translator[F, Event](
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x))))
    )
}
