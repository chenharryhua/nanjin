package com.github.chenharryhua.nanjin.kafka.serdes

import cats.effect.kernel.{Resource, Sync}
import fs2.kafka.{Deserializer, GenericDeserializer, GenericSerializer, KeyOrValue, Serializer}
import org.apache.kafka.common.serialization.Serde

/** A configured Kafka `Serde[A]`, tagged by the phantom `KV` (`Key` or `Value`) recording which side it was
  * registered for. Produced by `Unregistered.asKey`/`asValue` once a `SchemaRegistryClient` and properties
  * are available.
  */
opaque type Registered[KV <: KeyOrValue, A] = Serde[A]
object Registered:
  private[serdes] def apply[KV <: KeyOrValue, A](serde: Serde[A]): Registered[KV, A] = serde

  extension [KV <: KeyOrValue, A](rd: Registered[KV, A])

    /** The underlying Kafka `Serde[A]`. */
    def serde: Serde[A] = rd

    /** The fs2-kafka serializer as a `Resource`, closing the underlying serializer on release. The `KV` tag
      * carries through so a key serializer cannot be used where a value one is expected.
      */
    def serializer[F[_]](using F: Sync[F]): Resource[F, GenericSerializer[KV, F, A]] =
      Resource.make(F.delay(rd.serializer))(s => F.delay(s.close()))
        .map { ser =>
          Serializer.instance[F, A] { (topic, headers, a) =>
            F.delay(ser.serialize(topic, headers.asJava, a))
          }
        }

    /** The fs2-kafka deserializer as a `Resource`, closing the underlying deserializer on release. */
    def deserializer[F[_]](using F: Sync[F]): Resource[F, GenericDeserializer[KV, F, A]] =
      Resource.make(F.delay(rd.deserializer))(s => F.delay(s.close()))
        .map { deSer =>
          Deserializer.instance { (topic, headers, bytes) =>
            F.delay(deSer.deserialize(topic, headers.asJava, bytes))
          }
        }

end Registered
