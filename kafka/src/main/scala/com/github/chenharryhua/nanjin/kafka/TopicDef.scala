package com.github.chenharryhua.nanjin.kafka

import cats.kernel.Eq
import cats.syntax.eq.*
import cats.{Endo, Show}
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroFor, JsonFor, ProtobufFor}
import com.sksamuel.avro4s.Record
import fs2.kafka.ProducerRecord
import scalapb.GeneratedMessage

final class AvroTopic[K, V] private (val topicName: TopicName, val pair: AvroPair[K, V])
    extends Serializable {

  override def toString: String = topicName.value

  def withTopicName(tn: TopicNameL): AvroTopic[K, V] = new AvroTopic[K, V](TopicName(tn), pair)
  def modifyTopicName(f: Endo[String]): AvroTopic[K, V] =
    withTopicName(TopicName.unsafeFrom(f(topicName.value)).name)

  def producerRecord(k: K, v: V): ProducerRecord[K, V] = ProducerRecord(topicName.value, k, v)
  def genericRecord(k: K, v: V): Record = pair.producerFormat.toRecord(producerRecord(k, v))
}

object AvroTopic {

  implicit def showTopicDef[K, V]: Show[AvroTopic[K, V]] = Show.fromToString

  implicit def eqTopicDef[K, V]: Eq[AvroTopic[K, V]] =
    (x: AvroTopic[K, V], y: AvroTopic[K, V]) =>
      x.topicName.value === y.topicName.value &&
        x.pair.key.avroCodec.schema == y.pair.key.avroCodec.schema &&
        x.pair.value.avroCodec.schema == y.pair.value.avroCodec.schema

  def apply[K, V](key: AvroFor[K], value: AvroFor[V], topicName: TopicName): AvroTopic[K, V] =
    new AvroTopic(topicName, AvroPair(key, value))

  def apply[K: AvroFor, V: AvroFor](topicName: TopicName): AvroTopic[K, V] =
    new AvroTopic(topicName, AvroPair(AvroFor[K], AvroFor[V]))
}

final class ProtobufTopic[K <: GeneratedMessage, V <: GeneratedMessage] private (
  val topicName: TopicName,
  val pair: ProtobufPair[K, V])

object ProtobufTopic {
  def apply[K <: GeneratedMessage, V <: GeneratedMessage](
    key: ProtobufFor[K],
    value: ProtobufFor[V],
    topicName: TopicName): ProtobufTopic[K, V] =
    new ProtobufTopic[K, V](topicName, ProtobufPair[K, V](key, value))
}

final class JsonTopic[K, V] private (val topicName: TopicName, val pair: JsonPair[K, V])
object JsonTopic {
  def apply[K, V](key: JsonFor[K], value: JsonFor[V], topicName: TopicName) =
    new JsonTopic[K, V](topicName, JsonPair(key, value))
}
