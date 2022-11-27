package com.github.chenharryhua.nanjin.guard.service

import cats.effect.std.AtomicCell
import cats.Monad
import cats.data.State
import cats.effect.kernel.Ref
import cats.syntax.all.*
import fs2.concurrent.SignallingRef
import fs2.Stream
import org.typelevel.vault.{Key, Locker, Vault}

final private class NJAtomicBox[F[_], A](vault: AtomicCell[F, Vault], key: Key[A], initValue: F[A])(implicit
  F: Monad[F])
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

final private class NJSignalBox[F[_]: Monad, A](
  locker: SignallingRef[F, Option[Locker]],
  key: Key[A],
  initValue: F[A])
    extends SignallingRef[F, A] {

  private[this] val fsr: F[SignallingRef[F, A]] = initValue.map(init =>
    SignallingRef.lens[F, Option[Locker], A](locker)(
      (ol: Option[Locker]) => ol.flatMap(_.unlock(key)).getOrElse(init),
      (_: Option[Locker]) => (a: A) => Some(Locker(key, a))))

  override def discrete: Stream[F, A]                              = Stream.force(fsr.map(_.discrete))
  override def continuous: Stream[F, A]                            = Stream.force(fsr.map(_.continuous))
  override def get: F[A]                                           = fsr.flatMap(_.get)
  override def access: F[(A, A => F[Boolean])]                     = fsr.flatMap(_.access)
  override def tryUpdate(f: A => A): F[Boolean]                    = fsr.flatMap(_.tryUpdate(f))
  override def tryModify[B](f: A => (A, B)): F[Option[B]]          = fsr.flatMap(_.tryModify(f))
  override def update(f: A => A): F[Unit]                          = fsr.flatMap(_.update(f))
  override def modify[B](f: A => (A, B)): F[B]                     = fsr.flatMap(_.modify(f))
  override def tryModifyState[B](state: State[A, B]): F[Option[B]] = fsr.flatMap(_.tryModifyState(state))
  override def modifyState[B](state: State[A, B]): F[B]            = fsr.flatMap(_.modifyState(state))
  override def set(a: A): F[Unit]                                  = fsr.flatMap(_.set(a))
}

final private class NJRefBox[F[_]: Monad, A](locker: Ref[F, Option[Locker]], key: Key[A], initValue: F[A])
    extends Ref[F, A] {

  private[this] val fref: F[Ref[F, A]] = initValue.map(init =>
    Ref.lens[F, Option[Locker], A](locker)(
      (ol: Option[Locker]) => ol.flatMap(_.unlock(key)).getOrElse(init),
      (_: Option[Locker]) => (a: A) => Some(Locker(key, a))))

  override def access: F[(A, A => F[Boolean])]                     = fref.flatMap(_.access)
  override def tryUpdate(f: A => A): F[Boolean]                    = fref.flatMap(_.tryUpdate(f))
  override def tryModify[B](f: A => (A, B)): F[Option[B]]          = fref.flatMap(_.tryModify(f))
  override def update(f: A => A): F[Unit]                          = fref.flatMap(_.update(f))
  override def modify[B](f: A => (A, B)): F[B]                     = fref.flatMap(_.modify(f))
  override def tryModifyState[B](state: State[A, B]): F[Option[B]] = fref.flatMap(_.tryModifyState(state))
  override def modifyState[B](state: State[A, B]): F[B]            = fref.flatMap(_.modifyState(state))
  override def set(a: A): F[Unit]                                  = fref.flatMap(_.set(a))
  override def get: F[A]                                           = fref.flatMap(_.get)
}
