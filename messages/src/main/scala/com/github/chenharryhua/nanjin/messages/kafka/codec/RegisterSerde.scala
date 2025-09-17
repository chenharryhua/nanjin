package com.github.chenharryhua.nanjin.messages.kafka.codec

trait RegisterSerde[A] extends Serializable {
  def asKey(props: Map[String, String]): Registered[A]
  def asValue(props: Map[String, String]): Registered[A]
}
