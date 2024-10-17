package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Sync}
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import fs2.{Chunk, Pipe}
import io.lemonlabs.uri.Url

import java.time.ZoneId

trait HadoopSink[F[_], A] {
  def sink(path: Url)(implicit F: Sync[F]): Pipe[F, Chunk[A], Int]
  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => Url)(implicit
    F: Async[F]): Pipe[F, Chunk[A], Int]
}
