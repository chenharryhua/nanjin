package com.github.chenharryhua.nanjin.pipes.serde

import cats.effect.kernel.Async
import fs2.io.{readOutputStream, toInputStream}
import fs2.{Pipe, Stream}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import squants.information.Information

final class DelimitedProtoBufSerialization[F[_]](byteBuffer: Information) extends Serializable {

  def serialize[A](implicit cc: Async[F], ev: A <:< GeneratedMessage): Pipe[F, A, Byte] = { (ss: Stream[F, A]) =>
    readOutputStream[F](byteBuffer.toBytes.toInt) { os =>
      ss.map(_.writeDelimitedTo(os)).compile.drain
    }
  }

  def deserialize[A <: GeneratedMessage](implicit ce: Async[F], gmc: GeneratedMessageCompanion[A]): Pipe[F, Byte, A] =
    _.through(toInputStream[F]).flatMap { is =>
      Stream.fromIterator(gmc.streamFromDelimitedInput(is).iterator, byteBuffer.toBytes.toInt)
    }
}

final class ProtoBufSerialization[F[_]] extends Serializable {

  def serialize[A](implicit ev: A <:< GeneratedMessage): Pipe[F, A, Array[Byte]] =
    _.map(_.toByteArray)

  def deserialize[A <: GeneratedMessage](implicit gmc: GeneratedMessageCompanion[A]): Pipe[F, Array[Byte], A] =
    _.map(gmc.parseFrom)
}
