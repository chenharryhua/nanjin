package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.jdk.CollectionConverters.*
import scala.util.Try

/** [[https://github.com/sksamuel/avro4s]]
  */

/** @param topicName
  *   - topic name or state store name
  * @param registered
  *   serializer/deserializer config method was called
  * @tparam A
  *   schema related type
  */
final class KafkaSerde[A] private[codec] (val topicName: TopicName, val registered: Registered[A]) {
  def serialize(a: A): Array[Byte] = registered.serde.serializer.serialize(topicName.name.value, a)
  def deserialize(ab: Array[Byte]): A = registered.serde.deserializer.deserialize(topicName.name.value, ab)

  def tryDeserialize(ab: Array[Byte]): Try[A] = Try(deserialize(ab))
}

final class Registered[A] private[codec] (
  unregisteredSerde: Serde[A],
  props: Map[String, String],
  isKey: Boolean) {

  val serde: Serde[A] = new Serde[A] {
    override val serializer: Serializer[A] = {
      val ser = unregisteredSerde.serializer
      ser.configure(props.asJava, isKey)
      ser
    }
    override val deserializer: Deserializer[A] = {
      val deser = unregisteredSerde.deserializer
      deser.configure(props.asJava, isKey)
      deser
    }
  }

  def withTopic(topicName: TopicName): KafkaSerde[A] =
    new KafkaSerde[A](topicName, this)
}
