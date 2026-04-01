package com.github.chenharryhua.nanjin.guard.metrics.gauges

import cats.effect.kernel.{Async, Resource}
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.common.EnableConfig

trait IdleGauge[F[_]]:
  def wakeUp: F[Unit]

object IdleGauge:
  final class Builder private[IdleGauge] (isEnabled: Boolean) extends EnableConfig[Builder] {

    override def enable(isEnabled: Boolean): Builder = new Builder(isEnabled)

    def build[F[_]](gp: GaugeParams[F], name: String)(using F: Async[F]): Resource[F, IdleGauge[F]] =
      if isEnabled then
        for {
          lastUpdate <- Resource.eval(F.monotonic.flatMap(F.ref))
          _ <- Gauge(
            gp,
            name,
            _.withKind(_.Gauge).enable(isEnabled).register(
              for {
                pre <- lastUpdate.get
                now <- F.monotonic
              } yield defaultFormatter.format(now - pre)
            ))
        } yield new IdleGauge[F] {
          override val wakeUp: F[Unit] = F.monotonic.flatMap(lastUpdate.set)
        }
      else
        Resource.pure(new IdleGauge[F] {
          override def wakeUp: F[Unit] = F.unit
        })
  }

  def apply[F[_]: Async](gp: GaugeParams[F], name: String, f: Builder => Builder): Resource[F, IdleGauge[F]] =
    f(Builder(true)).build(gp, name)
