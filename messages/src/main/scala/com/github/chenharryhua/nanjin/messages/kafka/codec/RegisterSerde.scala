package com.github.chenharryhua.nanjin.messages.kafka.codec

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

trait RegisterSerde[A] extends Serializable { outer =>
  protected def serializer: Serializer[A]
  protected def deserializer: Deserializer[A]

  final def asKey(props: Map[String, String]): Registered[A] =
    new Registered[A](
      new Serde[A] with Serializable {
        override def serializer: Serializer[A] = outer.serializer
        override def deserializer: Deserializer[A] = outer.deserializer
      },
      props,
      true
    )

  final def asValue(props: Map[String, String]): Registered[A] =
    new Registered(
      new Serde[A] with Serializable {
        override def serializer: Serializer[A] = outer.serializer
        override def deserializer: Deserializer[A] = outer.deserializer
      },
      props,
      false
    )
}
