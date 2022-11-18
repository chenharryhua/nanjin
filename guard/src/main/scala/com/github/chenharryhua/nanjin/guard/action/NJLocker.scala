package com.github.chenharryhua.nanjin.guard.action

import cats.{Endo, Functor}
import cats.effect.std.AtomicCell
import cats.syntax.all.*
import org.typelevel.vault.{Key, Vault}

final class NJLocker[F[_]: Functor, A](vault: AtomicCell[F, Vault], key: Key[A], initValue: Option[A]) {
  val get: F[Option[A]]      = vault.get.map(_.lookup(key).orElse(initValue))
  def put(value: A): F[Unit] = vault.update(_.insert(key, value))
  def update(f: Endo[A]): F[Unit] =
    vault.update(vt => vt.lookup(key).orElse(initValue).map(a => vt.insert(key, f(a))).getOrElse(vt))
  def reset: F[Unit] = vault.update(_.delete(key))
}
