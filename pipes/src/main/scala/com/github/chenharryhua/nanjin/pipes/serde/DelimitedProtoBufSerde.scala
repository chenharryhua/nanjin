package com.github.chenharryhua.nanjin.pipes.serde

import cats.effect.kernel.Async
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.io.{readOutputStream, toInputStream}
import fs2.{Pipe, Stream}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import squants.information.Information

object DelimitedProtoBufSerde {

  def toBytes[F[_], A](byteBuffer: Information)(implicit cc: Async[F], ev: A <:< GeneratedMessage): Pipe[F, A, Byte] = {
    (ss: Stream[F, A]) =>
      readOutputStream[F](byteBuffer.toBytes.toInt) { os =>
        ss.map(_.writeDelimitedTo(os)).compile.drain
      }
  }

  def fromBytes[F[_], A <: GeneratedMessage](
    chunkSize: ChunkSize)(implicit ce: Async[F], gmc: GeneratedMessageCompanion[A]): Pipe[F, Byte, A] =
    _.through(toInputStream[F]).flatMap { is =>
      Stream.fromIterator(gmc.streamFromDelimitedInput(is).iterator, chunkSize.value)
    }
}

object ProtoBufSerde {

  def toBytes[F[_], A](implicit ev: A <:< GeneratedMessage): Pipe[F, A, Array[Byte]] =
    _.map(_.toByteArray)

  def fromBytes[F[_], A <: GeneratedMessage](implicit gmc: GeneratedMessageCompanion[A]): Pipe[F, Array[Byte], A] =
    _.map(gmc.parseFrom)
}
