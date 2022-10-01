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

  def run[A, Z](action: NJAction[F, A, Z])(a: A): F[Z]                           = action.run(a)
  def run[A, B, Z](action: NJAction[F, (A, B), Z])(a: A, b: B): F[Z]             = action.run((a, b), None)
  def run[A, B, C, Z](action: NJAction[F, (A, B, C), Z])(a: A, b: B, c: C): F[Z] = action.run(a, b, c)
  def run[A, B, C, D, Z](action: NJAction[F, (A, B, C, D), Z])(a: A, b: B, c: C, d: D): F[Z] =
    action.run(a, b, c, d)

  def run[A, B, C, D, E, Z](action: NJAction[F, (A, B, C, D, E), Z])(a: A, b: B, c: C, d: D, e: E): F[Z] =
    action.run(a, b, c, d, e)

}
