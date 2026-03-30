package com.github.chenharryhua.nanjin.guard.service

import cats.data.Kleisli
import com.github.chenharryhua.nanjin.guard.event.Event

private opaque type LogSink[F[_]] = Kleisli[F, Event, Unit]
private object LogSink:
  def apply[F[_]](fun: Event => F[Unit]): LogSink[F] = Kleisli(fun)
  extension [F[_]](ls: LogSink[F])
    inline def write(e: Event): F[Unit] = ls.run(e)
    def value: Kleisli[F, Event, Unit] = ls
