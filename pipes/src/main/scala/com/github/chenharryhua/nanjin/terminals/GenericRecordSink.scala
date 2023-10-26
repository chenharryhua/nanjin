package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Sync}
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import fs2.{Chunk, Pipe}
import org.apache.avro.generic.GenericRecord

import java.time.ZoneId

trait GenericRecordSink[F[_]] {
  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Chunk[GenericRecord], Nothing]

  def sink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Chunk[GenericRecord], Nothing]

}
