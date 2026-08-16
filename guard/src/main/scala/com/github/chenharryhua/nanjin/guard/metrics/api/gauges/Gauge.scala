package com.github.chenharryhua.nanjin.guard.metrics.api.gauges

import cats.Functor
import cats.effect.kernel.{Async, Resource}
import cats.effect.syntax.temporal.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.codahale.metrics.Gauge as CodahaleGauge
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy}
import com.github.chenharryhua.nanjin.guard.event.StackTrace
import com.github.chenharryhua.nanjin.guard.metrics.MetricCategoryKind.GaugeKind
import com.github.chenharryhua.nanjin.guard.metrics.{MetricCategory, MetricID, MetricName}
import io.circe.syntax.given
import io.circe.{Encoder, Json}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.control.NonFatal

/** Builder and registration support for gauges backed by an effectful value. */
object Gauge {

  final class Builder private[Gauge] (
    private[Gauge] val isEnabled: Boolean,
    private[Gauge] val timeout: FiniteDuration,
    private[Gauge] val policy: Option[Policy],
    private[Gauge] val kind: GaugeKind)
      extends EnableConfig[Builder] {
    /*
     * Transformation
     */
    /** Enable or disable gauge registration; disabled gauges become no-ops. */
    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, timeout, policy, kind)

    /** Bound evaluation time when the registry reads the gauge value. */
    def withTimeout(timeout: FiniteDuration): Builder =
      new Builder(isEnabled, timeout, policy, kind)

    /** Refresh the gauge value according to an optional policy. */
    def withPolicy(op: Option[Policy]): Builder =
      new Builder(isEnabled, timeout, op, kind)

    /** Refresh the gauge value according to a policy builder. */
    def withPolicy(f: Policy.type => Policy): Builder =
      withPolicy(Some(f(Policy)))

    private[gauges] def withKind(f: GaugeKind.type => GaugeKind): Builder =
      new Builder(isEnabled, timeout, policy, f(GaugeKind))

    // transition
    /** Register an effectful value and encode each result as JSON. */
    def register[F[_]: Functor, A: Encoder](fa: F[A]): Registered[F] =
      new Registered[F](builder = this, initial = fa.map(a => Encoder[A].apply(a)))
  }

  final class Registered[F[_]] private[Gauge] (builder: Builder, initial: F[Json]) {
    private def trans_error(ex: Throwable): Json = StackTrace(ex).headOption.asJson

    private def create(gp: GaugeParams[F], name: String, json: F[Json])(using
      F: Async[F]): Resource[F, Unit] = for {
      metricID <- Resource.eval(MetricName(name).map { metricName =>
        MetricID(gp.label, metricName, MetricCategory.GaugeC(builder.kind)).identifier
      })
      _ <- Resource.make(F.delay {
        gp.metricRegistry.gauge(
          metricID,
          () =>
            new CodahaleGauge[Json] {
              override def getValue: Json =
                try gp.dispatcher.unsafeRunTimed(json, builder.timeout)
                catch {
                  case NonFatal(ex) => trans_error(ex)
                }
            }
        )
      })(_ => F.delay(gp.metricRegistry.remove(metricID)).void)
    } yield ()

    private[Gauge] def build(gp: GaugeParams[F], name: String)(using F: Async[F]): Resource[F, Unit] =
      if builder.isEnabled then
        builder.policy match {
          case None         => create(gp, name, initial)
          case Some(policy) =>
            val handled: F[Json] = F.handleError(initial.timeout(builder.timeout))(trans_error)
            for {
              json <- Resource.eval(handled)
              ref <- Resource.eval(F.ref(json))
              _ <- F.background {
                tickStream.tickScheduled[F](gp.zoneId, _.fresh(policy))
                  .evalMap(_ => handled.flatMap(ref.set))
                  .compile.drain
              }
              _ <- create(gp, name, ref.get)
            } yield ()
        }
      else Resource.unit
  }

  private[metrics] def apply[F[_]: Async](
    gp: GaugeParams[F],
    name: String,
    f: Builder => Registered[F]): Resource[F, Unit] =
    f(new Builder(isEnabled = true, timeout = 5.seconds, policy = None, kind = GaugeKind.Default))
      .build(gp, name)
}
