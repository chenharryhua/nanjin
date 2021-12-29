package com.github.chenharryhua.nanjin.guard.observers

import alleycats.Pure
import cats.data.{Kleisli, OptionT}
import cats.syntax.all.*
import cats.{Applicative, Functor}
import com.github.chenharryhua.nanjin.guard.event.*
import io.circe.Json
import io.circe.generic.auto.*

final case class Translator[F[_], A] private (
  serviceStarted: Kleisli[OptionT[F, *], ServiceStarted, A],
  servicePanic: Kleisli[OptionT[F, *], ServicePanic, A],
  serviceStopped: Kleisli[OptionT[F, *], ServiceStopped, A],
  metricsReport: Kleisli[OptionT[F, *], MetricsReport, A],
  metricsReset: Kleisli[OptionT[F, *], MetricsReset, A],
  serviceAlert: Kleisli[OptionT[F, *], ServiceAlert, A],
  passThrough: Kleisli[OptionT[F, *], PassThrough, A],
  actionStart: Kleisli[OptionT[F, *], ActionStart, A],
  actionRetrying: Kleisli[OptionT[F, *], ActionRetrying, A],
  actionFailed: Kleisli[OptionT[F, *], ActionFailed, A],
  actionSucced: Kleisli[OptionT[F, *], ActionSucced, A]
) {
  def translate(event: NJEvent): F[Option[A]] = event match {
    case e: ServiceStarted => serviceStarted.run(e).value
    case e: ServicePanic   => servicePanic.run(e).value
    case e: ServiceStopped => serviceStopped.run(e).value
    case e: MetricsReport  => metricsReport.run(e).value
    case e: MetricsReset   => metricsReset.run(e).value
    case e: ServiceAlert   => serviceAlert.run(e).value
    case e: PassThrough    => passThrough.run(e).value
    case e: ActionStart    => actionStart.run(e).value
    case e: ActionRetrying => actionRetrying.run(e).value
    case e: ActionFailed   => actionFailed.run(e).value
    case e: ActionSucced   => actionSucced.run(e).value
  }
  def skipServiceStart(implicit F: Applicative[F]): Translator[F, A]   = copy(serviceStarted = Translator.noop[F, A])
  def skipServicePanic(implicit F: Applicative[F]): Translator[F, A]   = copy(servicePanic = Translator.noop[F, A])
  def skipServiceStopped(implicit F: Applicative[F]): Translator[F, A] = copy(serviceStopped = Translator.noop[F, A])
  def skipMetricsReport(implicit F: Applicative[F]): Translator[F, A]  = copy(metricsReport = Translator.noop[F, A])
  def skipMetricsReset(implicit F: Applicative[F]): Translator[F, A]   = copy(metricsReset = Translator.noop[F, A])
  def skipServiceAlert(implicit F: Applicative[F]): Translator[F, A]   = copy(serviceAlert = Translator.noop[F, A])
  def skipPassThrough(implicit F: Applicative[F]): Translator[F, A]    = copy(passThrough = Translator.noop[F, A])
  def skipActionStart(implicit F: Applicative[F]): Translator[F, A]    = copy(actionStart = Translator.noop[F, A])
  def skipActionRetrying(implicit F: Applicative[F]): Translator[F, A] = copy(actionRetrying = Translator.noop[F, A])
  def skipActionFailed(implicit F: Applicative[F]): Translator[F, A]   = copy(actionFailed = Translator.noop[F, A])
  def skipActionSucced(implicit F: Applicative[F]): Translator[F, A]   = copy(actionSucced = Translator.noop[F, A])
  def skipAll(implicit F: Applicative[F]): Translator[F, A]            = Translator.empty[F, A]

  def withServiceStarted(f: ServiceStarted => F[Option[A]]): Translator[F, A] =
    copy(serviceStarted = Kleisli(a => OptionT(f(a))))

  def withServiceStarted(f: ServiceStarted => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(serviceStarted = Kleisli(a => OptionT(F.pure(f(a)))))

  def withServiceStarted(f: ServiceStarted => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(serviceStarted = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withServiceStarted(f: ServiceStarted => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(serviceStarted = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withServicePanic(f: ServicePanic => F[Option[A]]): Translator[F, A] =
    copy(servicePanic = Kleisli(a => OptionT(f(a))))

  def withServicePanic(f: ServicePanic => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(servicePanic = Kleisli(a => OptionT(F.pure(f(a)))))

  def withServicePanic(f: ServicePanic => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(servicePanic = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withServicePanic(f: ServicePanic => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(servicePanic = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withServiceStopped(f: ServiceStopped => F[Option[A]]): Translator[F, A] =
    copy(serviceStopped = Kleisli(a => OptionT(f(a))))

  def withServiceStopped(f: ServiceStopped => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(serviceStopped = Kleisli(a => OptionT(F.pure(f(a)))))

  def withServiceStopped(f: ServiceStopped => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(serviceStopped = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withServiceStopped(f: ServiceStopped => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(serviceStopped = Kleisli(a => OptionT(F.pure(Some(f(a))))))

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

  def withServiceAlert(f: ServiceAlert => F[Option[A]]): Translator[F, A] =
    copy(serviceAlert = Kleisli(a => OptionT(f(a))))

  def withServiceAlert(f: ServiceAlert => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(serviceAlert = Kleisli(a => OptionT(F.pure(f(a)))))

  def withServiceAlert(f: ServiceAlert => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(serviceAlert = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withServiceAlert(f: ServiceAlert => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(serviceAlert = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withPassThrough(f: PassThrough => F[Option[A]]): Translator[F, A] =
    copy(passThrough = Kleisli(a => OptionT(f(a))))

  def withPassThrough(f: PassThrough => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(passThrough = Kleisli(a => OptionT(F.pure(f(a)))))

  def withPassThrough(f: PassThrough => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(passThrough = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withPassThrough(f: PassThrough => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(passThrough = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withActionStart(f: ActionStart => F[Option[A]]): Translator[F, A] =
    copy(actionStart = Kleisli(a => OptionT(f(a))))

  def withActionStart(f: ActionStart => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(actionStart = Kleisli(a => OptionT(F.pure(f(a)))))

  def withActionStart(f: ActionStart => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(actionStart = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withActionStart(f: ActionStart => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(actionStart = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withActionRetrying(f: ActionRetrying => F[Option[A]]): Translator[F, A] =
    copy(actionRetrying = Kleisli(a => OptionT(f(a))))

  def withActionRetrying(f: ActionRetrying => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(actionRetrying = Kleisli(a => OptionT(F.pure(f(a)))))

  def withActionRetrying(f: ActionRetrying => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(actionRetrying = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withActionRetrying(f: ActionRetrying => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(actionRetrying = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withActionFailed(f: ActionFailed => F[Option[A]]): Translator[F, A] =
    copy(actionFailed = Kleisli(a => OptionT(f(a))))

  def withActionFailed(f: ActionFailed => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(actionFailed = Kleisli(a => OptionT(F.pure(f(a)))))

  def withActionFailed(f: ActionFailed => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(actionFailed = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withActionFailed(f: ActionFailed => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(actionFailed = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withActionSucced(f: ActionSucced => F[Option[A]]): Translator[F, A] =
    copy(actionSucced = Kleisli(a => OptionT(f(a))))

  def withActionSucced(f: ActionSucced => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(actionSucced = Kleisli(a => OptionT(F.pure(f(a)))))

  def withActionSucced(f: ActionSucced => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(actionSucced = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withActionSucced(f: ActionSucced => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(actionSucced = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  // TODO
  // def flatMap[B](f: A => Translator[F, B])(implicit F: Monad[F]): Translator[F, B] = ???
}

object Translator {
  def noop[F[_], A](implicit F: Applicative[F]): Kleisli[OptionT[F, *], NJEvent, A] =
    Kleisli(_ => OptionT(F.pure(None)))

  def empty[F[_]: Applicative, A]: Translator[F, A] =
    Translator[F, A](
      noop[F, A],
      noop[F, A],
      noop[F, A],
      noop[F, A],
      noop[F, A],
      noop[F, A],
      noop[F, A],
      noop[F, A],
      noop[F, A],
      noop[F, A],
      noop[F, A]
    )

  def json[F[_]: Applicative]: Translator[F, Json] =
    empty[F, Json]
      .withServiceStarted(_.asJson)
      .withServicePanic(_.asJson)
      .withServiceStopped(_.asJson)
      .withServiceAlert(_.asJson)
      .withPassThrough(_.asJson)
      .withMetricsReset(_.asJson)
      .withMetricsReport(_.asJson)
      .withActionStart(_.asJson)
      .withActionRetrying(_.asJson)
      .withActionFailed(_.asJson)
      .withActionSucced(_.asJson)

  def text[F[_]: Applicative]: Translator[F, String] =
    empty[F, String]
      .withServiceStarted(_.show)
      .withServicePanic(_.show)
      .withServiceStopped(_.show)
      .withServiceAlert(_.show)
      .withPassThrough(_.show)
      .withMetricsReset(_.show)
      .withMetricsReport(_.show)
      .withActionStart(_.show)
      .withActionRetrying(_.show)
      .withActionFailed(_.show)
      .withActionSucced(_.show)
}
