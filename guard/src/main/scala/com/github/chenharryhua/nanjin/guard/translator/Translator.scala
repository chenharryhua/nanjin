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
  serviceMessage: Kleisli[OptionT[F, *], ServiceMessage, A],
  metricsReport: Kleisli[OptionT[F, *], MetricsReport, A],
  metricsReset: Kleisli[OptionT[F, *], MetricsReset, A]
) {

  def translate(event: Event): F[Option[A]] = event match {
    case e: ServiceStart   => serviceStart.run(e).value
    case e: ServicePanic   => servicePanic.run(e).value
    case e: ServiceStop    => serviceStop.run(e).value
    case e: MetricsReport  => metricsReport.run(e).value
    case e: MetricsReset   => metricsReset.run(e).value
    case e: ServiceMessage => serviceMessage.run(e).value
  }

  def filter(f: Event => Boolean)(implicit F: Applicative[F]): Translator[F, A] =
    Translator[F, A](
      Kleisli(ss => if (f(ss)) serviceStart.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) servicePanic.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) serviceStop.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) serviceMessage.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) metricsReport.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) metricsReset.run(ss) else OptionT(F.pure(None)))
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
  def skipMetricsReport(implicit F: Applicative[F]): Translator[F, A] =
    copy(metricsReport = Translator.noop[F, A])
  def skipMetricsReset(implicit F: Applicative[F]): Translator[F, A] =
    copy(metricsReset = Translator.noop[F, A])
  def skipServiceMessage(implicit F: Applicative[F]): Translator[F, A] =
    copy(serviceMessage = Translator.noop[F, A])

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

  def withMetricsReport(f: MetricsReport => F[Option[A]]): Translator[F, A] =
    copy(metricsReport = Kleisli(a => OptionT(f(a))))

  def withMetricsReport(f: MetricsReport => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(metricsReport = Kleisli(a => OptionT(F.pure(f(a)))))

  def withMetricsReport(f: MetricsReport => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(metricsReport = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withMetricsReport(f: MetricsReport => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(metricsReport = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withMetricsReset(f: MetricsReset => F[Option[A]]): Translator[F, A] =
    copy(metricsReset = Kleisli(a => OptionT(f(a))))

  def withMetricsReset(f: MetricsReset => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(metricsReset = Kleisli(a => OptionT(F.pure(f(a)))))

  def withMetricsReset(f: MetricsReset => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(metricsReset = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withMetricsReset(f: MetricsReset => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(metricsReset = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withServiceMessage(f: ServiceMessage => F[Option[A]]): Translator[F, A] =
    copy(serviceMessage = Kleisli(a => OptionT(f(a))))

  def withServiceMessage(f: ServiceMessage => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(serviceMessage = Kleisli(a => OptionT(F.pure(f(a)))))

  def withServiceMessage(f: ServiceMessage => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(serviceMessage = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withServiceMessage(f: ServiceMessage => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(serviceMessage = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def flatMap[B](f: A => Translator[F, B])(implicit F: Monad[F]): Translator[F, B] = {
    val go: Event => F[Option[Translator[F, B]]] = { (evt: Event) => translate(evt).map(_.map(f)) }
    Translator
      .empty[F, B]
      .withServiceStart(evt => go(evt).flatMap(_.flatTraverse(_.serviceStart.run(evt).value)))
      .withServicePanic(evt => go(evt).flatMap(_.flatTraverse(_.servicePanic.run(evt).value)))
      .withServiceStop(evt => go(evt).flatMap(_.flatTraverse(_.serviceStop.run(evt).value)))
      .withServiceMessage(evt => go(evt).flatMap(_.flatTraverse(_.serviceMessage.run(evt).value)))
      .withMetricsReport(evt => go(evt).flatMap(_.flatTraverse(_.metricsReport.run(evt).value)))
      .withMetricsReset(evt => go(evt).flatMap(_.flatTraverse(_.metricsReset.run(evt).value)))
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

        val metricsReport: Kleisli[OptionT[F, *], MetricsReport, B] =
          Kleisli((ss: MetricsReport) =>
            OptionT(F.tailRecM(a)(x => f(x).metricsReport.run(ss).value.map(mapper))))

        val metricsReset: Kleisli[OptionT[F, *], MetricsReset, B] =
          Kleisli((ss: MetricsReset) =>
            OptionT(F.tailRecM(a)(x => f(x).metricsReset.run(ss).value.map(mapper))))

        val serviceMessage: Kleisli[OptionT[F, *], ServiceMessage, B] =
          Kleisli((ss: ServiceMessage) =>
            OptionT(F.tailRecM(a)(x => f(x).serviceMessage.run(ss).value.map(mapper))))

        Translator[F, B](
          serviceStart,
          servicePanic,
          serviceStop,
          serviceMessage,
          metricsReport,
          metricsReset
        )
      }

      override def pure[A](x: A): Translator[F, A] =
        Translator[F, A](
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x)))),
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
          .withServiceMessage(go)
          .withMetricsReport(go)
          .withMetricsReset(go)
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
      noop[F, A],
      noop[F, A]
    )

  def idTranslator[F[_]](implicit F: Applicative[F]): Translator[F, Event] =
    Translator[F, Event](
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x))))
    )
}
