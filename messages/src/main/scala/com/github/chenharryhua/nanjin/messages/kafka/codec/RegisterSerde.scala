package com.github.chenharryhua.nanjin.messages.kafka.codec

import org.apache.kafka.common.serialization.Serde

trait RegisterSerde[A] extends Serializable { outer =>
  protected def serde: Serde[A]

  final def asKey(props: Map[String, String]): Registered[A] =
    new Registered[A](serde, props, true)

  final def asValue(props: Map[String, String]): Registered[A] =
    new Registered(serde, props, false)
}
