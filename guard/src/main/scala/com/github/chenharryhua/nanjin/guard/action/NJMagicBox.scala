package com.github.chenharryhua.nanjin.guard.action

import cats.{Functor, Monad}
import cats.effect.std.AtomicCell
import cats.syntax.all.*
import fs2.concurrent.{Signal, SignallingRef}
import fs2.Stream
import monocle.Iso
import org.typelevel.vault.{Key, Locker, Vault}

sealed trait NJMagicBox[F[_], A] {
  def get: F[A]
  def set(a: A): F[Unit]
  def update(f: A => A): F[Unit]
}

final class NJBlackBox[F[_]: Functor, A] private[guard] (
  locker: SignallingRef[F, Option[Locker]],
  key: Key[A],
  initValue: A)
    extends Signal[F, A] with NJMagicBox[F, A] {

  private val iso: Iso[Option[Locker], A] =
    Iso[Option[Locker], A](_.flatMap(_.unlock(key)).getOrElse(initValue))(a => Some(Locker(key, a)))

  private val signal: Signal[F, A]      = locker.map(iso.get)
  override def discrete: Stream[F, A]   = signal.discrete
  override def continuous: Stream[F, A] = signal.continuous
  override def get: F[A]                = signal.get

  override def update(f: A => A): F[Unit] =
    locker.update((ol: Option[Locker]) => iso.reverseGet(f(iso.get(ol))))
  override def set(a: A): F[Unit] = locker.set(iso.reverseGet(a))
}

final class NJAtomicBox[F[_]: Monad, A] private[guard] (
  vault: AtomicCell[F, Vault],
  key: Key[A],
  initValue: F[A])
    extends NJMagicBox[F, A] {
  override val get: F[A] = initValue.flatMap(init => vault.get.map(_.lookup(key).getOrElse(init)))
  override def set(value: A): F[Unit] = vault.update(_.insert(key, value))
  override def update(f: A => A): F[Unit] =
    for {
      init <- initValue
      _ <- vault.update { vt =>
        vt.lookup(key) match {
          case Some(value) => vt.insert(key, f(value))
          case None        => vt.insert(key, f(init))
        }
      }
    } yield ()
}
