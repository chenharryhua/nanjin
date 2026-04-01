package com.github.chenharryhua.nanjin.guard.metrics.gauges

import cats.effect.kernel.{Async, Resource}
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.option.{none, given}
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.common.EnableConfig

trait ActiveGauge[F[_]]:
  def deactivate: F[Unit]

object ActiveGauge:
  final class Builder(isEnabled: Boolean) extends EnableConfig[Builder] {

    override def enable(isEnabled: Boolean): Builder = new Builder(isEnabled)

    def build[F[_]](gp: GaugeParams[F], name: String)(using F: Async[F]): Resource[F, ActiveGauge[F]] =
      if isEnabled then
        for {
          active <- Resource.eval(F.ref(true))
          kickoff <- Resource.eval(F.monotonic)
          _ <- Gauge(
            gp,
            name,
            _.withKind(_.Gauge).enable(isEnabled).register(active.get
              .ifM(F.monotonic.map(now => defaultFormatter.format(now - kickoff).some), F.pure(none[String])))
          )
        } yield new ActiveGauge[F] {
          override val deactivate: F[Unit] = active.set(false)
        }
      else
        Resource.pure(new ActiveGauge[F] {
          override def deactivate: F[Unit] = F.unit
        })
  }

  def apply[F[_]: Async](
    gp: GaugeParams[F],
    name: String,
    f: Builder => Builder): Resource[F, ActiveGauge[F]] =
    f(Builder(true)).build(gp, name)
