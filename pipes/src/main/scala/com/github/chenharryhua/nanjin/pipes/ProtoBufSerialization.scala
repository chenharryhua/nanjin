package com.github.chenharryhua.nanjin.pipes

import cats.effect.ConcurrentEffect
import cats.implicits._
import fs2.io.toInputStream
import fs2.{Pipe, Stream}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

final class ProtoBufSerialization[F[_], A](implicit ev: A <:< GeneratedMessage) {

  def serialize: Pipe[F, A, Byte] = _.flatMap(a => Stream.emits(a.toByteArray))

}

final class ProtoBufDeserialization[F[_]: ConcurrentEffect, A <: GeneratedMessage](implicit
  ev: GeneratedMessageCompanion[A]) {

  def deserialize: Pipe[F, Byte, A] =
    _.through(toInputStream[F]).map(is => ev.parseFrom(is))
}
