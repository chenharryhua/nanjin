package com.github.chenharryhua.nanjin.messages.kafka

import cats.Eq
import cats.syntax.eq.catsSyntaxEq
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.{Header, Headers}

import java.util.Optional
import scala.jdk.OptionConverters.RichOptional

private[kafka] trait EqMessage {

  // kafka
  implicit val eqHeader: Eq[Header] = (x: Header, y: Header) =>
    x.key() === y.key() && x.value().sameElements(y.value())

  implicit val eqHeaders: Eq[Headers] = (x: Headers, y: Headers) => {
    val xa = x.toArray
    val ya = y.toArray
    xa.size === ya.size && xa.zip(ya).forall { case (x, y) => x === y }
  }

  implicit val eqOptionalInteger: Eq[Optional[java.lang.Integer]] =
    (x: Optional[Integer], y: Optional[Integer]) =>
      x.toScala.flatMap(Option(_).map(_.toInt)) === y.toScala.flatMap(Option(_).map(_.toInt))

  implicit val eqTopicPartition: Eq[TopicPartition] =
    (x: TopicPartition, y: TopicPartition) => x.equals(y)

  implicit val eqOffsetAndMetadata: Eq[OffsetAndMetadata] =
    (x: OffsetAndMetadata, y: OffsetAndMetadata) => x.equals(y)

  implicit final def eqConsumerRecord[K: Eq, V: Eq]: Eq[ConsumerRecord[K, V]] =
    (x: ConsumerRecord[K, V], y: ConsumerRecord[K, V]) =>
      x.topic() === y.topic &&
        x.partition() === y.partition() &&
        x.offset() === y.offset() &&
        x.timestamp() === y.timestamp() &&
        x.timestampType().id === y.timestampType().id &&
        x.serializedKeySize() === y.serializedKeySize() &&
        x.serializedValueSize() === y.serializedValueSize() &&
        x.key() === y.key() &&
        x.value() === y.value() &&
        x.headers() === y.headers() &&
        x.leaderEpoch() === y.leaderEpoch()

  implicit final def eqProducerRecord[K: Eq, V: Eq]: Eq[ProducerRecord[K, V]] =
    (x: ProducerRecord[K, V], y: ProducerRecord[K, V]) =>
      x.topic() === y.topic() &&
        x.partition().equals(y.partition()) &&
        x.timestamp().equals(y.timestamp()) &&
        x.key() === y.key() &&
        x.value() === y.value() &&
        x.headers() === y.headers()

}
