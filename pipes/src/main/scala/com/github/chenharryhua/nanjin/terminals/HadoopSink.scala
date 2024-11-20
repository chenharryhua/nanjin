package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Sync}
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickedValue}
import fs2.{Chunk, Pipe, Stream}
import io.lemonlabs.uri.Url

import java.time.ZoneId

trait HadoopSink[F[_], A] {
  def sink(path: Url)(implicit F: Sync[F]): Pipe[F, Chunk[A], Int]

  def sink(paths: Stream[F, TickedValue[Url]])(implicit F: Async[F]): Pipe[F, Chunk[A], TickedValue[Int]]

  final def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => Url)(implicit
    F: Async[F]): Pipe[F, Chunk[A], TickedValue[Int]] =
    sink(tickStream.fromZero[F](policy, zoneId).map(tick => TickedValue(tick, pathBuilder(tick))))
}
