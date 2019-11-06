package com.github.chenharryhua.nanjin.codec

import cats.effect.Concurrent
import cats.effect.concurrent.MVar
import cats.implicits._

sealed trait Synchronized[F[_], A] {
  def use[B](f: A => F[B]): F[B]
}

object Synchronized {

  def apply[F[_]](implicit F: Concurrent[F]): PartiallyApplyBuilder[F] =
    new PartiallyApplyBuilder(F)

  def of[F[_], A](a: A)(implicit F: Concurrent[F]): F[Synchronized[F, A]] =
    MVar[F]
      .of[A](a)
      .map(mv =>
        new Synchronized[F, A] {
          override def use[B](f: A => F[B]): F[B] = F.bracket(mv.take)(f)(v => mv.put(v))
        })

  final class PartiallyApplyBuilder[F[_]](private val concurrent: Concurrent[F]) extends AnyVal {

    def of[A](a: A): F[Synchronized[F, A]] =
      Synchronized.of(a)(concurrent)
  }
}
