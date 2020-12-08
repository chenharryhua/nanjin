package com.github.chenharryhua.nanjin.pipes

import cats.effect.{Blocker, Concurrent, ConcurrentEffect, ContextShift}
import com.google.protobuf.CodedInputStream
import fs2.io.{readOutputStream, toInputStream}
import fs2.{Pipe, Stream}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

final class DelimitedProtoBufSerialization[F[_]] extends Serializable {

  def serialize[A](blocker: Blocker)(implicit
    cc: Concurrent[F],
    cs: ContextShift[F],
    ev: A <:< GeneratedMessage): Pipe[F, A, Byte] = { (ss: Stream[F, A]) =>
    readOutputStream[F](blocker, chunkSize) { os =>
      ss.map(_.writeDelimitedTo(os)).compile.drain
    }
  }

  def deserialize[A <: GeneratedMessage](implicit
    ce: ConcurrentEffect[F],
    gmc: GeneratedMessageCompanion[A]): Pipe[F, Byte, A] =
    _.through(toInputStream[F]).flatMap { is =>
      Stream.fromIterator(gmc.streamFromDelimitedInput(is).toIterator)
    }
}

final class ProtoBufSerialization[F[_]] extends Serializable {

  def serialize[A](implicit ev: A <:< GeneratedMessage): Pipe[F, A, Array[Byte]] =
    _.map(_.toByteArray)

  def deserialize[A <: GeneratedMessage](implicit
    gmc: GeneratedMessageCompanion[A]): Pipe[F, Array[Byte], A] =
    _.map(gmc.parseFrom)
}
