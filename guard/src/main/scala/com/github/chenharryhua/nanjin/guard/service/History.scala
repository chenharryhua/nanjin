package com.github.chenharryhua.nanjin.guard.service

import cats.Applicative
import cats.effect.kernel.{Concurrent, Ref}
import cats.syntax.applicative.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.guard.config.Capacity

sealed private trait History[F[_], A] {
  def add(a: A): F[Unit]
  def value: F[Vector[A]]
  def clear: F[Unit]
}

private object History {
  final private class Impl[F[_], A](vector: Ref[F, Vector[A]], max: Capacity) extends History[F, A] {
    override def add(a: A): F[Unit] =
      vector.update { v =>
        if (v.size >= max.value) v.tail :+ a else v :+ a
      }

    override val value: F[Vector[A]] = vector.get

    override val clear: F[Unit] = vector.set(Vector.empty)
  }

  private def noop[F[_], A](using F: Applicative[F]): History[F, A] =
    new History[F, A] {
      override def add(a: A): F[Unit] = F.unit
      override val value: F[Vector[A]] = Vector.empty[A].pure
      override val clear: F[Unit] = F.unit
    }

  def apply[F[_]: Concurrent, A](max: Option[Capacity]): F[History[F, A]] =
    max match {
      case Some(value) if value.value <= 0 => noop.pure
      case Some(value) => Ref.of[F, Vector[A]](Vector.empty[A]).map(new Impl[F, A](_, value))
      case None        => noop.pure
    }
}
