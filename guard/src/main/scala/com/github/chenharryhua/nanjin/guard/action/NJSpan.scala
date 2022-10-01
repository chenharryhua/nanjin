package com.github.chenharryhua.nanjin.guard.action
import cats.effect.kernel.Async
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import fs2.concurrent.Channel

final class NJSpan[F[_]] private[guard] (
  val spanName: String,
  val parent: Option[NJSpan[F]],
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent])(implicit F: Async[F]) {

  // private lazy val ancestors: List[String] = LazyList.unfold(this)(_.parent.map(p => (p.spanName, p))).toList

  def child(name: String): NJSpan[F] =
    new NJSpan[F](name, Some(this), metricRegistry, channel)

}
