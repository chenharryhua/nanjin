package com.github.chenharryhua.nanjin.messages.kafka.codec

import org.apache.kafka.common.serialization.Serde

trait RegisterSerde[A] { outer =>
  protected val unregisteredSerde: Serde[A]

  final def asKey(props: Map[String, String]): Registered[A] =
    new Registered[A](unregisteredSerde, props, true)

  final def asValue(props: Map[String, String]): Registered[A] =
    new Registered(unregisteredSerde, props, false)
}
