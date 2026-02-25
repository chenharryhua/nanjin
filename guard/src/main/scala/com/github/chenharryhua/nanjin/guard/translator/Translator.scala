package com.github.chenharryhua.nanjin.guard.translator

import alleycats.Pure
import cats.data.{Kleisli, OptionT}
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.traverse.toTraverseOps
import cats.{Applicative, Endo, Functor, FunctorFilter, Monad, Traverse}
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.*
import monocle.macros.Lenses

trait UpdateTranslator[F[_], A, B] {
  def updateTranslator(f: Endo[Translator[F, A]]): B
}

@Lenses final case class Translator[F[_], A](
  serviceStart: Kleisli[OptionT[F, *], ServiceStart, A],
  servicePanic: Kleisli[OptionT[F, *], ServicePanic, A],
  serviceStop: Kleisli[OptionT[F, *], ServiceStop, A],
  reportedEvent: Kleisli[OptionT[F, *], ReportedEvent, A],
  metricsSnapshot: Kleisli[OptionT[F, *], MetricsSnapshot, A]
) {

  def translate(event: Event): F[Option[A]] = event match {
    case e: ServiceStart    => serviceStart.run(e).value
    case e: ServicePanic    => servicePanic.run(e).value
    case e: ServiceStop     => serviceStop.run(e).value
    case e: MetricsSnapshot => metricsSnapshot.run(e).value
    case e: ReportedEvent   => reportedEvent.run(e).value
  }

  def filter(f: Event => Boolean)(implicit F: Applicative[F]): Translator[F, A] =
    Translator[F, A](
      Kleisli(ss => if (f(ss)) serviceStart.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) servicePanic.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) serviceStop.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) reportedEvent.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) metricsSnapshot.run(ss) else OptionT(F.pure(None)))
    )

  // for convenience
  def traverse[G[_]](ge: G[Event])(implicit F: Applicative[F], G: Traverse[G]): F[G[Option[A]]] =
    G.traverse[F, Event, Option[A]](ge)(translate)

  def skipServiceStart(implicit F: Applicative[F]): Translator[F, A] =
    copy(serviceStart = Translator.noop[F, A])
  def skipServicePanic(implicit F: Applicative[F]): Translator[F, A] =
    copy(servicePanic = Translator.noop[F, A])
  def skipServiceStop(implicit F: Applicative[F]): Translator[F, A] =
    copy(serviceStop = Translator.noop[F, A])
  def skipMetricsSnapshot(implicit F: Applicative[F]): Translator[F, A] =
    copy(metricsSnapshot = Translator.noop[F, A])
  def skipReportedEvent(implicit F: Applicative[F]): Translator[F, A] =
    copy(reportedEvent = Translator.noop[F, A])

  def skipAll(implicit F: Applicative[F]): Translator[F, A] =
    Translator.empty[F, A]

  def withServiceStart(f: ServiceStart => F[Option[A]]): Translator[F, A] =
    copy(serviceStart = Kleisli(a => OptionT(f(a))))

  def withServiceStart(f: ServiceStart => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(serviceStart = Kleisli(a => OptionT(F.pure(f(a)))))

  def withServiceStart(f: ServiceStart => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(serviceStart = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withServiceStart(f: ServiceStart => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(serviceStart = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withServicePanic(f: ServicePanic => F[Option[A]]): Translator[F, A] =
    copy(servicePanic = Kleisli(a => OptionT(f(a))))

  def withServicePanic(f: ServicePanic => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(servicePanic = Kleisli(a => OptionT(F.pure(f(a)))))

  def withServicePanic(f: ServicePanic => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(servicePanic = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withServicePanic(f: ServicePanic => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(servicePanic = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withServiceStop(f: ServiceStop => F[Option[A]]): Translator[F, A] =
    copy(serviceStop = Kleisli(a => OptionT(f(a))))

  def withServiceStop(f: ServiceStop => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(serviceStop = Kleisli(a => OptionT(F.pure(f(a)))))

  def withServiceStop(f: ServiceStop => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(serviceStop = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withServiceStop(f: ServiceStop => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(serviceStop = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withMetricsSnapshot(f: MetricsSnapshot => F[Option[A]]): Translator[F, A] =
    copy(metricsSnapshot = Kleisli(a => OptionT(f(a))))

  def withMetricsSnapshot(f: MetricsSnapshot => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(metricsSnapshot = Kleisli(a => OptionT(F.pure(f(a)))))

  def withMetricsSnapshot(f: MetricsSnapshot => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(metricsSnapshot = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withMetricsSnapshot(f: MetricsSnapshot => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(metricsSnapshot = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withReportedEvent(f: ReportedEvent => F[Option[A]]): Translator[F, A] =
    copy(reportedEvent = Kleisli(a => OptionT(f(a))))

  def withReportedEvent(f: ReportedEvent => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(reportedEvent = Kleisli(a => OptionT(F.pure(f(a)))))

  def withReportedEvent(f: ReportedEvent => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(reportedEvent = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withReportedEvent(f: ReportedEvent => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(reportedEvent = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def flatMap[B](f: A => Translator[F, B])(implicit F: Monad[F]): Translator[F, B] = {
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
  implicit final def monadTranslator[F[_]](implicit
    F: Monad[F]): Monad[Translator[F, *]] & FunctorFilter[Translator[F, *]] =
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

  def noop[F[_], A](implicit F: Applicative[F]): Kleisli[OptionT[F, *], Event, A] =
    Kleisli(_ => OptionT(F.pure(None)))

  def empty[F[_]: Applicative, A]: Translator[F, A] =
    Translator[F, A](
      noop[F, A],
      noop[F, A],
      noop[F, A],
      noop[F, A],
      noop[F, A]
    )

  def idTranslator[F[_]](implicit F: Applicative[F]): Translator[F, Event] =
    Translator[F, Event](
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x))))
    )
}
