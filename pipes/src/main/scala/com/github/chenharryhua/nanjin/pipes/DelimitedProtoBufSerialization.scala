package com.github.chenharryhua.nanjin.pipes

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift}
import com.google.protobuf.CodedInputStream
import fs2.io.{readOutputStream, toInputStream}
import fs2.{Pipe, Stream}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

final class DelimitedProtoBufSerialization[F[_], A <: GeneratedMessage] extends Serializable {

  def serialize(blocker: Blocker)(implicit
    cc: Concurrent[F],
    cs: ContextShift[F]): Pipe[F, A, Byte] = { (ss: Stream[F, A]) =>
    readOutputStream[F](blocker, chunkSize) { os =>
      ss.map(_.writeDelimitedTo(os)).compile.drain
    }
  }

  def deserialize(implicit
    ce: ConcurrentEffect[F],
    ev: GeneratedMessageCompanion[A]): Pipe[F, Byte, A] =
    _.through(toInputStream[F]).flatMap { is =>
      val cis = CodedInputStream.newInstance(is)
      Stream.repeatEval(ConcurrentEffect[F].delay(ev.parseDelimitedFrom(cis))).unNoneTerminate
    }
}

final class ProtoBufSerialization[F[_], A <: GeneratedMessage] extends Serializable {

  def serialize(implicit ev: A <:< GeneratedMessage): Pipe[F, A, Array[Byte]] =
    _.map(_.toByteArray)

  def deserialize(implicit ev: GeneratedMessageCompanion[A]): Pipe[F, Array[Byte], A] =
    _.map(ev.parseFrom)
}
