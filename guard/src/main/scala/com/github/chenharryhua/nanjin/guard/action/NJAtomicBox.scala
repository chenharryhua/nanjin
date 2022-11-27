package com.github.chenharryhua.nanjin.guard.action

import cats.effect.std.AtomicCell
import cats.Monad
import cats.syntax.all.*
import org.typelevel.vault.{Key, Vault}

final class NJAtomicBox[F[_], A] private[guard] (vault: AtomicCell[F, Vault], key: Key[A], initValue: F[A])(
  implicit F: Monad[F])
    extends AtomicCell[F, A] {

  override def evalModify[B](f: A => F[(A, B)]): F[B] =
    vault.evalModify { vt =>
      vt.lookup(key) match {
        case Some(value) => f(value).map(ab => (vt.insert(key, ab._1), ab._2))
        case None =>
          for {
            init <- initValue
            (a, b) <- f(init)
          } yield (vt.insert(key, a), b)
      }
    }

  override def modify[B](f: A => (A, B)): F[B]   = evalModify((a: A) => F.pure(f(a)))
  override def evalUpdate(f: A => F[A]): F[Unit] = evalModify((a: A) => f(a).map((_, ())))
  override def update(f: A => A): F[Unit]        = evalUpdate((a: A) => F.pure(f(a)))

  override def evalGetAndUpdate(f: A => F[A]): F[A] = evalModify((a: A) => f(a).map((_, a)))
  override def evalUpdateAndGet(f: A => F[A]): F[A] = evalModify((a: A) => f(a).map(b => (b, b)))

  override def getAndUpdate(f: A => A): F[A] = evalGetAndUpdate((a: A) => F.pure(f(a)))
  override def updateAndGet(f: A => A): F[A] = evalUpdateAndGet((a: A) => F.pure(f(a)))

  override def get: F[A]          = getAndUpdate(identity)
  override def set(a: A): F[Unit] = update(_ => a)
}
