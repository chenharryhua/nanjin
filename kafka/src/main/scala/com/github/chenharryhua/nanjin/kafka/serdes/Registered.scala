package com.github.chenharryhua.nanjin.kafka.serdes

import cats.effect.kernel.{Resource, Sync}
import fs2.kafka.{GenericDeserializer, GenericSerializer, KeyOrValue}
import org.apache.kafka.common.serialization.Serde

opaque type Registered[KV <: KeyOrValue, A] = Serde[A]
object Registered:
  private[serdes] def apply[KV <: KeyOrValue, A](serde: Serde[A]): Registered[KV, A] = serde

  extension [KV <: KeyOrValue, A](rd: Registered[KV, A])
    def serde: Serde[A] = rd

    def serializer[F[_]](using F: Sync[F]): Resource[F, GenericSerializer[KV, F, A]] =
      Resource.make(F.delay(rd.serializer))(s => F.delay(s.close()))
        .map(GenericSerializer.delegate(_))

    def deserializer[F[_]](using F: Sync[F]): Resource[F, GenericDeserializer[KV, F, A]] =
      Resource.make(F.delay(rd.deserializer))(s => F.delay(s.close()))
        .map(GenericDeserializer.delegate(_))

end Registered
