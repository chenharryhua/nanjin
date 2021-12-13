package com.github.chenharryhua.nanjin.kafka.streaming

import cats.effect.kernel.Deferred
import cats.effect.std.Dispatcher
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler

trait UncaughtErrorHandler[F[_]] {

  /** @param dispatcher
    *   - run side-effect
    * @param notifier
    *   - terminate stream when error happens, normally Right(()), abnormally Left(Throwable)
    */
  def apply(dispatcher: Dispatcher[F], notifier: Deferred[F, Either[Throwable, Unit]]): StreamsUncaughtExceptionHandler
}

object UncaughtErrorHandler {
  def default[F[_]]: UncaughtErrorHandler[F] = new UncaughtErrorHandler[F] {
    override def apply(
      dispatcher: Dispatcher[F],
      notifier: Deferred[F, Either[Throwable, Unit]]): StreamsUncaughtExceptionHandler =
      new StreamsUncaughtExceptionHandler {
        override def handle(throwable: Throwable): StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse = {
          dispatcher.unsafeRunSync(notifier.complete(Left(throwable)))
          StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION
        }
      }
  }
}
