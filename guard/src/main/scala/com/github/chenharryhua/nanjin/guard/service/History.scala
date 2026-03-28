package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Concurrent, Ref}
import cats.syntax.functor.*

final private class History[F[_], A] private (vector: Ref[F, Vector[A]], max: Int) {
  def add(a: A): F[Unit] =
    vector.update { v =>
      if (v.size >= max) v.tail :+ a else v :+ a
    }

  val value: F[Vector[A]] = vector.get

  val clear: F[Unit] = vector.set(Vector.empty)
}

private object History:
  def apply[F[_]: Concurrent, A](max: Int): F[History[F, A]] =
    Ref.of[F, Vector[A]](Vector.empty[A]).map(new History[F, A](_, max))
