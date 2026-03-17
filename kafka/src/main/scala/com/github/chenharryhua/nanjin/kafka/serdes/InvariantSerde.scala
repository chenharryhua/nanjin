package com.github.chenharryhua.nanjin.kafka.serdes

import cats.Invariant
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

/*
 * WARNING: Not a lawful functor.
 *
 * Kafka Serdes are effectful (serializer/deserializer can throw, mutate state, or depend on config),
 * so functor/invariant laws cannot hold. This `imap` is only for type-safe transformation of the underlying Serde.
 */
given Invariant[Serde] = new Invariant[Serde] {
  override def imap[A, B](fa: Serde[A])(f: A => B)(g: B => A): Serde[B] =
    new Serde[B] {
      /*
       * Serializer
       */
      override def serializer: Serializer[B] = new Serializer[B] {
        override def serialize(topic: String, data: B): Array[Byte] =
          fa.serializer.serialize(topic, g(data))
        override def serialize(topic: String, headers: Headers, data: B): Array[Byte] =
          fa.serializer.serialize(topic, headers, g(data))
      }

      /*
       * Deserializer
       */
      override def deserializer: Deserializer[B] = new Deserializer[B] {
        override def deserialize(topic: String, data: Array[Byte]): B =
          f(fa.deserializer.deserialize(topic, data))

        override def deserialize(topic: String, headers: Headers, data: Array[Byte]): B =
          f(fa.deserializer.deserialize(topic, headers, data))
      }
    }
}
