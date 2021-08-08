package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.common.metrics.NJMetricReporter
import com.github.chenharryhua.nanjin.guard.alert.AlertService

private[guard] trait AddAlertService[F[_], A] {
  def addAlertService(ras: Resource[F, AlertService[F]]): A

  final def addAlertService(as: AlertService[F]): A =
    addAlertService(Resource.pure[F, AlertService[F]](as))
}

private[guard] trait AddMetricReporter[A] {
  def addMetricReporter(reporter: NJMetricReporter): A
}
