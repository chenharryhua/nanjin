package com.github.chenharryhua.nanjin.guard.action

import cats.Endo
import cats.effect.kernel.Sync
import cats.effect.std.AtomicCell
import cats.syntax.all.*
import org.typelevel.vault.{Key, Vault}

final class NJLocker[F[_], A](vault: AtomicCell[F, Vault], key: Key[A], initValue: A)(implicit F: Sync[F]) {
  val get: F[A]              = F.defer(vault.get.map(_.lookup(key).getOrElse(initValue)))
  def put(value: A): F[Unit] = F.defer(vault.update(_.insert(key, value)))
  def update(f: Endo[A]): F[Unit] = F.defer(vault.update { vt =>
    vt.lookup(key) match {
      case Some(value) => vt.insert(key, f(value))
      case None        => vt.insert(key, f(initValue))
    }
  })
  def reset: F[Unit] = F.defer(vault.update(_.delete(key)))
}

final class NJLockerOption[F[_], A](vault: AtomicCell[F, Vault], key: Key[A])(implicit F: Sync[F]) {
  val get: F[Option[A]]           = F.defer(vault.get.map(_.lookup(key)))
  def put(value: A): F[Unit]      = F.defer(vault.update(_.insert(key, value)))
  def update(f: Endo[A]): F[Unit] = F.defer(vault.update(_.adjust(key, f)))
  def reset: F[Unit]              = F.defer(vault.update(_.delete(key)))
}
