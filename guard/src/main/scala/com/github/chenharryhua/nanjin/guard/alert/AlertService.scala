package com.github.chenharryhua.nanjin.guard.alert

import cats.syntax.all._
import cats.{Applicative, Monoid}

trait AlertService[F[_]] {
  def alert(event: NJEvent): F[Unit]
}

object AlertService {

  implicit def monoidAlertService[F[_]](implicit F: Applicative[F]): Monoid[AlertService[F]] =
    new Monoid[AlertService[F]] {

      override def empty: AlertService[F] = (event: NJEvent) => F.unit

      override def combine(x: AlertService[F], y: AlertService[F]): AlertService[F] =
        (event: NJEvent) => F.product(x.alert(event), y.alert(event)).void
    }
}
