package com.github.chenharryhua.nanjin.guard.logging

import cats.syntax.apply.catsSyntaxApplyOps
import cats.{Applicative, Semigroup}
import io.circe.Encoder

trait Log[F[_]] {

  def error[S: Encoder](msg: S): F[Unit]
  def error[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def warn[S: Encoder](msg: S): F[Unit]
  def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit]

  def done[S: Encoder](msg: S): F[Unit]

  def info[S: Encoder](msg: S): F[Unit]

  def debug[S: Encoder](msg: S): F[Unit]
  def debug[S: Encoder](msg: => F[S]): F[Unit]

  def void[S](msg: S): F[Unit]
}

object Log {
  implicit def semigroupLog[F[_]: Applicative]: Semigroup[Log[F]] = new Semigroup[Log[F]] {
    override def combine(x: Log[F], y: Log[F]): Log[F] = new Log[F] {
      override def error[S: Encoder](msg: S): F[Unit] = x.error(msg) *> y.error(msg)
      override def error[S: Encoder](ex: Throwable)(msg: S): F[Unit] = x.error(ex)(msg) *> y.error(ex)(msg)

      override def warn[S: Encoder](msg: S): F[Unit] = x.warn(msg) *> y.warn(msg)
      override def warn[S: Encoder](ex: Throwable)(msg: S): F[Unit] = x.warn(ex)(msg) *> y.warn(ex)(msg)

      override def done[S: Encoder](msg: S): F[Unit] = x.done(msg) *> y.done(msg)

      override def info[S: Encoder](msg: S): F[Unit] = x.info(msg) *> y.info(msg)

      override def debug[S: Encoder](msg: S): F[Unit] = x.debug(msg) *> y.debug(msg)
      override def debug[S: Encoder](msg: => F[S]): F[Unit] = x.debug(msg) *> y.debug(msg)

      override def void[S](msg: S): F[Unit] = Applicative[F].unit
    }
  }
}
