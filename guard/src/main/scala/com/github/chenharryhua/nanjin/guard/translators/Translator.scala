package com.github.chenharryhua.nanjin.guard.translators

import alleycats.Pure
import cats.data.{Kleisli, OptionT}
import cats.syntax.all.*
import cats.{Applicative, Functor, FunctorFilter, Monad, Traverse}
import com.github.chenharryhua.nanjin.guard.event.*
import io.circe.Json
import io.circe.generic.auto.*
import monocle.macros.Lenses
import scalatags.Text

trait UpdateTranslator[F[_], A, B] {
  def updateTranslator(f: Translator[F, A] => Translator[F, A]): B
}

@Lenses final case class Translator[F[_], A] private (
  serviceStart: Kleisli[OptionT[F, *], ServiceStart, A],
  servicePanic: Kleisli[OptionT[F, *], ServicePanic, A],
  serviceStop: Kleisli[OptionT[F, *], ServiceStop, A],
  metricReport: Kleisli[OptionT[F, *], MetricReport, A],
  metricReset: Kleisli[OptionT[F, *], MetricReset, A],
  instantAlert: Kleisli[OptionT[F, *], InstantAlert, A],
  passThrough: Kleisli[OptionT[F, *], PassThrough, A],
  actionStart: Kleisli[OptionT[F, *], ActionStart, A],
  actionRetry: Kleisli[OptionT[F, *], ActionRetry, A],
  actionFail: Kleisli[OptionT[F, *], ActionFail, A],
  actionSucc: Kleisli[OptionT[F, *], ActionSucc, A]
) {

  def translate(event: NJEvent): F[Option[A]] = event match {
    case e: ServiceStart => serviceStart.run(e).value
    case e: ServicePanic => servicePanic.run(e).value
    case e: ServiceStop  => serviceStop.run(e).value
    case e: MetricReport => metricReport.run(e).value
    case e: MetricReset  => metricReset.run(e).value
    case e: InstantAlert => instantAlert.run(e).value
    case e: PassThrough  => passThrough.run(e).value
    case e: ActionStart  => actionStart.run(e).value
    case e: ActionRetry  => actionRetry.run(e).value
    case e: ActionFail   => actionFail.run(e).value
    case e: ActionSucc   => actionSucc.run(e).value
  }

  def filter(f: NJEvent => Boolean)(implicit F: Applicative[F]): Translator[F, A] =
    Translator[F, A](
      Kleisli(ss => if (f(ss)) serviceStart.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) servicePanic.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) serviceStop.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) metricReport.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) metricReset.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) instantAlert.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) passThrough.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) actionStart.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) actionRetry.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) actionFail.run(ss) else OptionT(F.pure(None))),
      Kleisli(ss => if (f(ss)) actionSucc.run(ss) else OptionT(F.pure(None)))
    )

  // for convenience
  def traverse[G[_]](ge: G[NJEvent])(implicit F: Applicative[F], G: Traverse[G]): F[G[Option[A]]] =
    G.traverse(ge)(translate)

  def skipServiceStart(implicit F: Applicative[F]): Translator[F, A]  = copy(serviceStart = Translator.noop[F, A])
  def skipServicePanic(implicit F: Applicative[F]): Translator[F, A]  = copy(servicePanic = Translator.noop[F, A])
  def skipServiceStop(implicit F: Applicative[F]): Translator[F, A]   = copy(serviceStop = Translator.noop[F, A])
  def skipMetricsReport(implicit F: Applicative[F]): Translator[F, A] = copy(metricReport = Translator.noop[F, A])
  def skipMetricsReset(implicit F: Applicative[F]): Translator[F, A]  = copy(metricReset = Translator.noop[F, A])
  def skipInstantAlert(implicit F: Applicative[F]): Translator[F, A]  = copy(instantAlert = Translator.noop[F, A])
  def skipPassThrough(implicit F: Applicative[F]): Translator[F, A]   = copy(passThrough = Translator.noop[F, A])
  def skipActionStart(implicit F: Applicative[F]): Translator[F, A]   = copy(actionStart = Translator.noop[F, A])
  def skipActionRetry(implicit F: Applicative[F]): Translator[F, A]   = copy(actionRetry = Translator.noop[F, A])
  def skipActionFail(implicit F: Applicative[F]): Translator[F, A]    = copy(actionFail = Translator.noop[F, A])
  def skipActionSucc(implicit F: Applicative[F]): Translator[F, A]    = copy(actionSucc = Translator.noop[F, A])
  def skipAll(implicit F: Applicative[F]): Translator[F, A]           = Translator.empty[F, A]

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

  def withMetricsReport(f: MetricReport => F[Option[A]]): Translator[F, A] =
    copy(metricReport = Kleisli(a => OptionT(f(a))))

  def withMetricsReport(f: MetricReport => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(metricReport = Kleisli(a => OptionT(F.pure(f(a)))))

  def withMetricsReport(f: MetricReport => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(metricReport = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withMetricsReport(f: MetricReport => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(metricReport = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withMetricsReset(f: MetricReset => F[Option[A]]): Translator[F, A] =
    copy(metricReset = Kleisli(a => OptionT(f(a))))

  def withMetricsReset(f: MetricReset => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(metricReset = Kleisli(a => OptionT(F.pure(f(a)))))

  def withMetricsReset(f: MetricReset => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(metricReset = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withMetricsReset(f: MetricReset => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(metricReset = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withInstantAlert(f: InstantAlert => F[Option[A]]): Translator[F, A] =
    copy(instantAlert = Kleisli(a => OptionT(f(a))))

  def withInstantAlert(f: InstantAlert => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(instantAlert = Kleisli(a => OptionT(F.pure(f(a)))))

  def withInstantAlert(f: InstantAlert => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(instantAlert = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withInstantAlert(f: InstantAlert => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(instantAlert = Kleisli(a => OptionT(F.pure(Some(f(a))))))

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

  def withActionRetry(f: ActionRetry => F[Option[A]]): Translator[F, A] =
    copy(actionRetry = Kleisli(a => OptionT(f(a))))

  def withActionRetry(f: ActionRetry => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(actionRetry = Kleisli(a => OptionT(F.pure(f(a)))))

  def withActionRetry(f: ActionRetry => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(actionRetry = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withActionRetry(f: ActionRetry => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(actionRetry = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withActionFail(f: ActionFail => F[Option[A]]): Translator[F, A] =
    copy(actionFail = Kleisli(a => OptionT(f(a))))

  def withActionFail(f: ActionFail => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(actionFail = Kleisli(a => OptionT(F.pure(f(a)))))

  def withActionFail(f: ActionFail => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(actionFail = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withActionFail(f: ActionFail => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(actionFail = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def withActionSucc(f: ActionSucc => F[Option[A]]): Translator[F, A] =
    copy(actionSucc = Kleisli(a => OptionT(f(a))))

  def withActionSucc(f: ActionSucc => Option[A])(implicit F: Applicative[F]): Translator[F, A] =
    copy(actionSucc = Kleisli(a => OptionT(F.pure(f(a)))))

  def withActionSucc(f: ActionSucc => F[A])(implicit F: Functor[F]): Translator[F, A] =
    copy(actionSucc = Kleisli(a => OptionT(f(a).map(Some(_)))))

  def withActionSucc(f: ActionSucc => A)(implicit F: Pure[F]): Translator[F, A] =
    copy(actionSucc = Kleisli(a => OptionT(F.pure(Some(f(a))))))

  def flatMap[B](f: A => Translator[F, B])(implicit F: Monad[F]): Translator[F, B] = {
    val g: NJEvent => F[Option[Translator[F, B]]] = { (evt: NJEvent) => translate(evt).map(_.map(f)) }
    Translator
      .empty[F, B]
      .withServiceStart(evt => g(evt).flatMap(_.flatTraverse(_.serviceStart.run(evt).value)))
      .withServicePanic(evt => g(evt).flatMap(_.flatTraverse(_.servicePanic.run(evt).value)))
      .withServiceStop(evt => g(evt).flatMap(_.flatTraverse(_.serviceStop.run(evt).value)))
      .withInstantAlert(evt => g(evt).flatMap(_.flatTraverse(_.instantAlert.run(evt).value)))
      .withPassThrough(evt => g(evt).flatMap(_.flatTraverse(_.passThrough.run(evt).value)))
      .withMetricsReport(evt => g(evt).flatMap(_.flatTraverse(_.metricReport.run(evt).value)))
      .withMetricsReset(evt => g(evt).flatMap(_.flatTraverse(_.metricReset.run(evt).value)))
      .withActionStart(evt => g(evt).flatMap(_.flatTraverse(_.actionStart.run(evt).value)))
      .withActionRetry(evt => g(evt).flatMap(_.flatTraverse(_.actionRetry.run(evt).value)))
      .withActionFail(evt => g(evt).flatMap(_.flatTraverse(_.actionFail.run(evt).value)))
      .withActionSucc(evt => g(evt).flatMap(_.flatTraverse(_.actionSucc.run(evt).value)))
  }
}

object Translator {
  implicit def monadTranslator[F[_]](implicit F: Monad[F]): Monad[Translator[F, *]] & FunctorFilter[Translator[F, *]] =
    new Monad[Translator[F, *]] with FunctorFilter[Translator[F, *]] {
      override def flatMap[A, B](fa: Translator[F, A])(f: A => Translator[F, B]): Translator[F, B] = fa.flatMap(f)

      override def tailRecM[A, B](a: A)(f: A => Translator[F, Either[A, B]]): Translator[F, B] = {
        def mapper(oeab: Option[Either[A, B]]): Either[A, Option[B]] =
          oeab match {
            case None           => Right(None)
            case Some(Right(r)) => Right(Some(r))
            case Some(Left(l))  => Left(l)
          }

        val serviceStart: Kleisli[OptionT[F, *], ServiceStart, B] =
          Kleisli((ss: ServiceStart) => OptionT(F.tailRecM(a)(x => f(x).serviceStart.run(ss).value.map(mapper))))

        val servicePanic: Kleisli[OptionT[F, *], ServicePanic, B] =
          Kleisli((ss: ServicePanic) => OptionT(F.tailRecM(a)(x => f(x).servicePanic.run(ss).value.map(mapper))))

        val serviceStop: Kleisli[OptionT[F, *], ServiceStop, B] =
          Kleisli((ss: ServiceStop) => OptionT(F.tailRecM(a)(x => f(x).serviceStop.run(ss).value.map(mapper))))

        val metricsReport: Kleisli[OptionT[F, *], MetricReport, B] =
          Kleisli((ss: MetricReport) => OptionT(F.tailRecM(a)(x => f(x).metricReport.run(ss).value.map(mapper))))

        val metricsReset: Kleisli[OptionT[F, *], MetricReset, B] =
          Kleisli((ss: MetricReset) => OptionT(F.tailRecM(a)(x => f(x).metricReset.run(ss).value.map(mapper))))

        val instantAlert: Kleisli[OptionT[F, *], InstantAlert, B] =
          Kleisli((ss: InstantAlert) => OptionT(F.tailRecM(a)(x => f(x).instantAlert.run(ss).value.map(mapper))))

        val passThrough: Kleisli[OptionT[F, *], PassThrough, B] =
          Kleisli((ss: PassThrough) => OptionT(F.tailRecM(a)(x => f(x).passThrough.run(ss).value.map(mapper))))

        val actionStart: Kleisli[OptionT[F, *], ActionStart, B] =
          Kleisli((ss: ActionStart) => OptionT(F.tailRecM(a)(x => f(x).actionStart.run(ss).value.map(mapper))))

        val actionRetry: Kleisli[OptionT[F, *], ActionRetry, B] =
          Kleisli((ss: ActionRetry) => OptionT(F.tailRecM(a)(x => f(x).actionRetry.run(ss).value.map(mapper))))

        val actionFail: Kleisli[OptionT[F, *], ActionFail, B] =
          Kleisli((ss: ActionFail) => OptionT(F.tailRecM(a)(x => f(x).actionFail.run(ss).value.map(mapper))))

        val actionSucc: Kleisli[OptionT[F, *], ActionSucc, B] =
          Kleisli((ss: ActionSucc) => OptionT(F.tailRecM(a)(x => f(x).actionSucc.run(ss).value.map(mapper))))

        Translator[F, B](
          serviceStart,
          servicePanic,
          serviceStop,
          metricsReport,
          metricsReset,
          instantAlert,
          passThrough,
          actionStart,
          actionRetry,
          actionFail,
          actionSucc)
      }

      override def pure[A](x: A): Translator[F, A] =
        Translator[F, A](
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x)))),
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x)))),
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x)))),
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x)))),
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x)))),
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x)))),
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x)))),
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x)))),
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x)))),
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x)))),
          Kleisli(_ => OptionT(F.pure[Option[A]](Some(x))))
        )

      override val functor: Functor[Translator[F, *]] = this

      override def mapFilter[A, B](fa: Translator[F, A])(f: A => Option[B]): Translator[F, B] = {
        def go(e: NJEvent): F[Option[B]] = fa.translate(e).map(_.flatMap(f))
        Translator
          .empty[F, B]
          .withServiceStart(go)
          .withServicePanic(go)
          .withServiceStop(go)
          .withInstantAlert(go)
          .withPassThrough(go)
          .withMetricsReport(go)
          .withMetricsReset(go)
          .withActionStart(go)
          .withActionRetry(go)
          .withActionFail(go)
          .withActionSucc(go)
      }
    }

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

  def idTranslator[F[_]](implicit F: Applicative[F]): Translator[F, NJEvent] =
    Translator[F, NJEvent](
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x)))),
      Kleisli(x => OptionT(F.pure(Some(x))))
    )

  def json[F[_]: Applicative]: Translator[F, Json] =
    empty[F, Json]
      .withServiceStart(_.asJson)
      .withServicePanic(_.asJson)
      .withServiceStop(_.asJson)
      .withInstantAlert(_.asJson)
      .withPassThrough(_.asJson)
      .withMetricsReset(_.asJson)
      .withMetricsReport(_.asJson)
      .withActionStart(_.asJson)
      .withActionRetry(_.asJson)
      .withActionFail(_.asJson)
      .withActionSucc(_.asJson)

  def text[F[_]: Applicative]: Translator[F, String] =
    empty[F, String]
      .withServiceStart(_.show)
      .withServicePanic(_.show)
      .withServiceStop(_.show)
      .withInstantAlert(_.show)
      .withPassThrough(_.show)
      .withMetricsReset(_.show)
      .withMetricsReport(_.show)
      .withActionStart(_.show)
      .withActionRetry(_.show)
      .withActionFail(_.show)
      .withActionSucc(_.show)

  def simpleText[F[_]: Applicative]: Translator[F, String]    = SimpleTextTranslator[F]
  def simpleJson[F[_]: Applicative]: Translator[F, Json]      = SimpleJsonTranslator[F]
  def html[F[_]: Monad]: Translator[F, Text.TypedTag[String]] = HtmlTranslator[F]
  def slack[F[_]: Applicative]: Translator[F, SlackApp]       = SlackTranslator[F]
}
