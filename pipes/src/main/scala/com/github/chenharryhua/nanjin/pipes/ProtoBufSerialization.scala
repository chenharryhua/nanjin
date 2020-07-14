package com.github.chenharryhua.nanjin.pipes

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift}
import cats.implicits._
import com.google.protobuf.CodedInputStream
import fs2.io.{readOutputStream, toInputStream}
import fs2.{Pipe, Stream}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

final class ProtoBufSerialization[F[_]: Concurrent: ContextShift, A](blocker: Blocker)(implicit
  ev: A <:< GeneratedMessage) {

  def serialize: Pipe[F, A, Byte] = { (ss: Stream[F, A]) =>
    readOutputStream[F](blocker, chunkSize) { os =>
      ss.map(_.writeDelimitedTo(os)).compile.drain
    }
  }
}

final class ProtoBufDeserialization[F[_]: ConcurrentEffect, A <: GeneratedMessage](implicit
  ev: GeneratedMessageCompanion[A]) {

  def deserialize: Pipe[F, Byte, A] =
    _.through(toInputStream[F]).flatMap { is =>
      val cis = CodedInputStream.newInstance(is)
      Stream.repeatEval(ConcurrentEffect[F].delay(ev.parseDelimitedFrom(cis))).unNoneTerminate
    }
}
