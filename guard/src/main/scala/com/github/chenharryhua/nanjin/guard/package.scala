package com.github.chenharryhua.nanjin

import cats.effect.kernel.Temporal
import cats.syntax.functor.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ServiceParams

import java.time.ZonedDateTime

package object guard {
  private[guard] def realZonedDateTime[F[_]](serviceParams: ServiceParams)(implicit F: Temporal[F]): F[ZonedDateTime] =
    F.realTimeInstant.map(_.atZone(serviceParams.taskParams.zoneId))

  private[guard] def actionStart(name: String, mr: MetricRegistry): Unit = mr.counter(s"05.count.[$name]").inc()
  //private[guard] def actionRetry(name: String, mr: MetricRegistry): Unit = mr.counter(s"06.retry.[$name]").inc()
  private[guard] def actionSucc(name: String, mr: MetricRegistry): Unit = mr.counter(s"07.succ.[$name]").inc()
}
