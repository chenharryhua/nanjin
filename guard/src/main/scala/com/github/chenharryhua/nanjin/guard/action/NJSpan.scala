package com.github.chenharryhua.nanjin.guard.action
import cats.effect.kernel.Temporal
import cats.effect.Resource
import natchez.{Kernel, Span, TraceValue}

import java.net.URI

final class NJSpan[F[_]] private[guard] (name: String, rawSpan: Span[F])(implicit F: Temporal[F])
    extends Span[F] {
  override def put(fields: (String, TraceValue)*): F[Unit] = rawSpan.put(fields*)
  override def kernel: F[Kernel]                           = rawSpan.kernel
  override def span(name: String): Resource[F, NJSpan[F]]  = rawSpan.span(name).map(new NJSpan[F](name, _))
  override def traceId: F[Option[String]]                  = rawSpan.traceId
  override def spanId: F[Option[String]]                   = rawSpan.spanId
  override def traceUri: F[Option[URI]]                    = rawSpan.traceUri

  // private lazy val ancestors: List[String] = LazyList.unfold(this)(_.parent.map(p => (p.spanName, p))).toList

  def run[Z](action: NJAction0[F, Z]): F[Z] =
    rawSpan.span(name).use(span => action.run(name, span))
  def run[A, Z](action: NJAction[F, A, Z])(a: A): F[Z] =
    rawSpan.span(name).use(span => action.apply(name, a, Some(span)))
  def run[A, B, Z](action: NJAction[F, (A, B), Z])(a: A, b: B): F[Z] =
    rawSpan.span(name).use(span => action.apply(name, (a, b), Some(span)))
  def run[A, B, C, Z](action: NJAction[F, (A, B, C), Z])(a: A, b: B, c: C): F[Z] =
    rawSpan.span(name).use(span => action.apply(name, (a, b, c), Some(span)))

  def run[A, B, C, D, Z](action: NJAction[F, (A, B, C, D), Z])(a: A, b: B, c: C, d: D): F[Z] =
    rawSpan.span(name).use(span => action.apply(name, (a, b, c, d), Some(span)))

  def run[A, B, C, D, E, Z](action: NJAction[F, (A, B, C, D, E), Z])(a: A, b: B, c: C, d: D, e: E): F[Z] =
    rawSpan.span(name).use(span => action.apply(name, (a, b, c, d, e), Some(span)))
}
