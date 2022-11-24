package com.github.chenharryhua.nanjin.guard.action

import cats.{Functor, Monad}
import cats.effect.std.AtomicCell
import fs2.concurrent.{Signal, SignallingRef}
import fs2.Stream
import monocle.Iso
import org.typelevel.vault.{Key, Locker, Vault}

/** Box which survives service panic
  */

sealed trait NJMagicBox[F[_], A] {
  def get: F[A]
  def update(f: A => A): F[Unit]
  def getAndUpdate(f: A => A): F[A]
  def updateAndGet(f: A => A): F[A]

  final def getAndSet(a: A): F[A] = getAndUpdate(_ => a)
  final def set(a: A): F[Unit]    = update(_ => a)
}

/** SignallingRef-like structure, but unable to modify @tparam A
  */

final class NJSignalBox[F[_], A] private[guard] (
  locker: SignallingRef[F, Option[Locker]],
  key: Key[A],
  initValue: A)(implicit F: Functor[F])
    extends Signal[F, A] with NJMagicBox[F, A] {

  private val iso: Iso[Option[Locker], A] =
    Iso[Option[Locker], A](_.flatMap(_.unlock(key)).getOrElse(initValue))(a => Some(Locker(key, a)))

  private val signal: Signal[F, A]      = locker.map(iso.get)
  override def discrete: Stream[F, A]   = signal.discrete
  override def continuous: Stream[F, A] = signal.continuous
  override def get: F[A]                = signal.get

  override def update(f: A => A): F[Unit] =
    locker.update((ol: Option[Locker]) => iso.reverseGet(f(iso.get(ol))))

  override def getAndUpdate(f: A => A): F[A] =
    F.map(locker.getAndUpdate((old: Option[Locker]) => iso.reverseGet(f(iso.get(old)))))(iso.get)

  override def updateAndGet(f: A => A): F[A] =
    F.map(locker.updateAndGet((old: Option[Locker]) => iso.reverseGet(f(iso.get(old)))))(iso.get)

  // specific for signalBox
  def tryUpdate(f: A => A): F[Boolean] =
    locker.tryUpdate((ol: Option[Locker]) => iso.reverseGet(f(iso.get(ol))))

  def access: F[(A, A => F[Boolean])] =
    F.map(locker.access) { case (ol, f) =>
      (iso.get(ol), (a: A) => f(iso.reverseGet(a)))
    }
}

/** AtomicCell-like structure, but unable to modify @tparam A
  */
final class NJAtomicBox[F[_], A] private[guard] (vault: AtomicCell[F, Vault], key: Key[A], initValue: F[A])(
  implicit F: Monad[F])
    extends NJMagicBox[F, A] {

  override def get: F[A]                  = getAndUpdate(identity)
  override def update(f: A => A): F[Unit] = evalUpdate((a: A) => F.pure(f(a)))

  override def getAndUpdate(f: A => A): F[A] = evalGetAndUpdate((a: A) => F.pure(f(a)))
  override def updateAndGet(f: A => A): F[A] = evalUpdateAndGet((a: A) => F.pure(f(a)))

  // specific for atomicBox
  def evalUpdate(f: A => F[A]): F[Unit] =
    F.flatMap(initValue) { init =>
      vault.evalModify { vt =>
        val old = vt.lookup(key).getOrElse(init)
        F.map(f(old))(newA => (vt.insert(key, newA), ()))
      }
    }

  def evalGetAndUpdate(f: A => F[A]): F[A] =
    F.flatMap(initValue) { init =>
      vault.evalModify { vt =>
        val old = vt.lookup(key).getOrElse(init)
        F.map(f(old))(newA => (vt.insert(key, newA), old))
      }
    }

  def evalUpdateAndGet(f: A => F[A]): F[A] =
    F.flatMap(initValue) { init =>
      vault.evalModify { vt =>
        val old = vt.lookup(key).getOrElse(init)
        F.map(f(old))(newA => (vt.insert(key, newA), newA))
      }
    }
}
