package com.github.chenharryhua.nanjin.utils

import cats.effect.Async
import fs2.Stream
import fs2.concurrent.Signal
import org.jline.terminal.{Terminal, TerminalBuilder}

object Keyboard {
  val pauSe: Char    = 's'
  val Quit: Char     = 'q'
  val Continue: Char = 'c'

  def signal[F[_]](implicit F: Async[F]): Stream[F, Signal[F, Option[Char]]] =
    Stream
      .bracket(F.delay {
        val terminal: Terminal = TerminalBuilder
          .builder()
          .nativeSignals(true)
          .signalHandler(Terminal.SignalHandler.SIG_IGN)
          .jna(true)
          .system(true)
          .build()
        terminal.enterRawMode
        (terminal, terminal.reader)
      }) { case (terminal, reader) => F.delay { reader.close(); terminal.close() } }
      .flatMap { case (_, r) => Stream.repeatEval(F.delay(r.read().toChar)) }
      .noneTerminate
      .hold(None)
}
