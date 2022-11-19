package com.github.chenharryhua.nanjin.guard.action

import cats.{Endo, Functor}
import cats.effect.std.AtomicCell
import cats.syntax.all.*
import org.typelevel.vault.{Key, Vault}

final class NJLocker[F[_]: Functor, A](vault: AtomicCell[F, Vault], key: Key[A], initValue: A) {
  val get: F[A]              = vault.get.map(_.lookup(key).getOrElse(initValue))
  def put(value: A): F[Unit] = vault.update(_.insert(key, value))
  def update(f: Endo[A]): F[Unit] = vault.update { vt =>
    vt.lookup(key) match {
      case Some(value) =>
        vt.insert(key, f(value))
      case None =>
        vt.insert(key, f(initValue))
    }
  }
  def reset: F[Unit] = vault.update(_.delete(key))
}

final class NJLockerOption[F[_]: Functor, A](vault: AtomicCell[F, Vault], key: Key[A]) {
  val get: F[Option[A]]           = vault.get.map(_.lookup(key))
  def put(value: A): F[Unit]      = vault.update(_.insert(key, value))
  def update(f: Endo[A]): F[Unit] = vault.update(_.adjust(key, f))
  def reset: F[Unit]              = vault.update(_.delete(key))
}
