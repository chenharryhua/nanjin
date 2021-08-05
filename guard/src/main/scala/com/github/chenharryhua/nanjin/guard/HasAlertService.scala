package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.guard.alert.AlertService

private[guard] trait HasAlertService[F[_], A] {
  def withAlert(ras: Resource[F, AlertService[F]]): A

  final def withAlert(as: AlertService[F]): A =
    withAlert(Resource.pure[F, AlertService[F]](as))
}
