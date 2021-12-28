package com.github.chenharryhua.nanjin.guard.observers

import cats.Applicative
import cats.data.{Kleisli, OptionT}
import com.github.chenharryhua.nanjin.guard.event.*

final case class Translator[F[_], A](
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

  def withServiceStarted(f: ServiceStarted => F[Option[A]]): Translator[F, A] =
    copy(serviceStarted = Kleisli(a => OptionT(f(a))))

  def withServiceStarted(f: ServiceStarted => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(serviceStarted = Kleisli(a => OptionT(F.pure(f(a)))))

  def withServicePanic(f: ServicePanic => F[Option[A]]): Translator[F, A] =
    copy(servicePanic = Kleisli(a => OptionT(f(a))))

  def withServicePanic(f: ServicePanic => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(servicePanic = Kleisli(a => OptionT(F.pure(f(a)))))

  def withServiceStopped(f: ServiceStopped => F[Option[A]]): Translator[F, A] =
    copy(serviceStopped = Kleisli(a => OptionT(f(a))))

  def withServiceStopped(f: ServiceStopped => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(serviceStopped = Kleisli(a => OptionT(F.pure(f(a)))))

  def withMetricsReport(f: MetricsReport => F[Option[A]]): Translator[F, A] =
    copy(metricsReport = Kleisli(a => OptionT(f(a))))

  def withMetricsReport(f: MetricsReport => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(metricsReport = Kleisli(a => OptionT(F.pure(f(a)))))

  def withMetricsReset(f: MetricsReset => F[Option[A]]): Translator[F, A] =
    copy(metricsReset = Kleisli(a => OptionT(f(a))))

  def withMetricsReset(f: MetricsReset => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(metricsReset = Kleisli(a => OptionT(F.pure(f(a)))))

  def withServiceAlert(f: ServiceAlert => F[Option[A]]): Translator[F, A] =
    copy(serviceAlert = Kleisli(a => OptionT(f(a))))

  def withServiceAlert(f: ServiceAlert => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(serviceAlert = Kleisli(a => OptionT(F.pure(f(a)))))

  def withPassThrough(f: PassThrough => F[Option[A]]): Translator[F, A] =
    copy(passThrough = Kleisli(a => OptionT(f(a))))

  def withPassThrough(f: PassThrough => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(passThrough = Kleisli(a => OptionT(F.pure(f(a)))))

  def withActionStart(f: ActionStart => F[Option[A]]): Translator[F, A] =
    copy(actionStart = Kleisli(a => OptionT(f(a))))

  def withActionStart(f: ActionStart => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(actionStart = Kleisli(a => OptionT(F.pure(f(a)))))

  def withActionRetrying(f: ActionRetrying => F[Option[A]]): Translator[F, A] =
    copy(actionRetrying = Kleisli(a => OptionT(f(a))))

  def withActionRetrying(f: ActionRetrying => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(actionRetrying = Kleisli(a => OptionT(F.pure(f(a)))))

  def withActionFailed(f: ActionFailed => F[Option[A]]): Translator[F, A] =
    copy(actionFailed = Kleisli(a => OptionT(f(a))))

  def withActionFailed(f: ActionFailed => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(actionFailed = Kleisli(a => OptionT(F.pure(f(a)))))

  def withActionSucced(f: ActionSucced => F[Option[A]]): Translator[F, A] =
    copy(actionSucced = Kleisli(a => OptionT(f(a))))

  def withActionSucced(f: ActionSucced => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(actionSucced = Kleisli(a => OptionT(F.pure(f(a)))))
}
