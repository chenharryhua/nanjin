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

  def runAction[Z](action: NJAction0[F, Z]): F[Z] =
    action.run(name, this)
  def runAction[A, Z](action: NJAction[F, A, Z])(a: A): F[Z] =
    action.apply(name, a, Some(this))
  def runAction[A, B, Z](action: NJAction[F, (A, B), Z])(a: A, b: B): F[Z] =
    action.apply(name, (a, b), Some(this))
  def runAction[A, B, C, Z](action: NJAction[F, (A, B, C), Z])(a: A, b: B, c: C): F[Z] =
    action.apply(name, (a, b, c), Some(this))
  def runAction[A, B, C, D, Z](action: NJAction[F, (A, B, C, D), Z])(a: A, b: B, c: C, d: D): F[Z] =
    action.apply(name, (a, b, c, d), Some(this))
  def runAction[A, B, C, D, E, Z](
    action: NJAction[F, (A, B, C, D, E), Z])(a: A, b: B, c: C, d: D, e: E): F[Z] =
    action.apply(name, (a, b, c, d, e), Some(this))
}
