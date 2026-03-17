package com.github.chenharryhua.nanjin.kafka.serdes

import cats.effect.kernel.Sync
import fs2.kafka.{GenericDeserializer, GenericSerializer, KeyOrValue}
import org.apache.kafka.common.serialization.Serde

opaque type Registered[KV <: KeyOrValue, A] = Serde[A]
object Registered:
  private[serdes] def apply[KV <: KeyOrValue, A](serde: Serde[A]): Registered[KV, A] = serde

  extension [KV <: KeyOrValue, A](rd: Registered[KV, A])
    def serde: Serde[A] = rd

    def serializer[F[_]: Sync]: GenericSerializer[KV, F, A] =
      GenericSerializer.delegate[F, A](rd.serializer)

    def deserializer[F[_]: Sync]: GenericDeserializer[KV, F, A] =
      GenericDeserializer.delegate[F, A](rd.deserializer)
end Registered
