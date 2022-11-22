package com.github.chenharryhua.nanjin.guard.action

import cats.Functor
import fs2.concurrent.{Signal, SignallingRef}
import fs2.Stream
import monocle.Iso
import org.typelevel.vault.{Key, Locker}

final class NJBlackBox[F[_]: Functor, A](locker: SignallingRef[F, Option[Locker]], key: Key[A], initValue: A)
    extends Signal[F, A] {

  private val iso: Iso[Option[Locker], A] =
    Iso[Option[Locker], A](_.flatMap(_.unlock(key)).getOrElse(initValue))(a => Some(Locker(key, a)))

  private val signal: Signal[F, A]      = locker.map(iso.get)
  override def discrete: Stream[F, A]   = signal.discrete
  override def continuous: Stream[F, A] = signal.continuous
  override def get: F[A]                = signal.get

  def update(f: A => A): F[Unit] = locker.update((ol: Option[Locker]) => iso.reverseGet(f(iso.get(ol))))
  def set(a: A): F[Unit]         = locker.set(iso.reverseGet(a))
}
