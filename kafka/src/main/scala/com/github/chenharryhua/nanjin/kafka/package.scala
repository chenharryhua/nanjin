package com.github.chenharryhua.nanjin

import cats.effect.kernel.{Resource, Sync}
import cats.{Order, Show}
import fs2.kafka.consumer.MkConsumer
import fs2.kafka.{ConsumerSettings, Id, KafkaByteConsumer, KeyDeserializer, ValueDeserializer}
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.kafka.clients.consumer.CloseOptions
import org.apache.kafka.common.TopicPartition

import scala.jdk.DurationConverters.ScalaDurationOps

package object kafka {
  final private val TOPIC: String = "topic"
  final private val PARTITION: String = "partition"

  given Ordering[TopicPartition] = Ordering.by(tp => (tp.topic(), tp.partition()))
  given Order[TopicPartition] = Order.fromOrdering[TopicPartition]

  given Show[TopicPartition] = Show.fromToString[TopicPartition]

  given Encoder[TopicPartition] = (a: TopicPartition) =>
    Json.obj(TOPIC -> Json.fromString(a.topic()), PARTITION -> Json.fromInt(a.partition()))

  given Decoder[TopicPartition] = (c: HCursor) =>
    for {
      topic <- c.downField(TOPIC).as[String]
      partition <- c.downField(PARTITION).as[Int]
    } yield new TopicPartition(topic, partition)

  type PureConsumerSettings = ConsumerSettings[Id, Null, Null]
  val PureConsumerSettings: PureConsumerSettings =
    ConsumerSettings[Id, Null, Null](
      keyDeserializer = null: KeyDeserializer[Id, Null],
      valueDeserializer = null: ValueDeserializer[Id, Null]
    )

  private[kafka] def makePureConsumer[F[_]: Sync](cs: PureConsumerSettings): Resource[F, KafkaByteConsumer] =
    Resource.make(MkConsumer.mkConsumerForSync[F].apply(cs))(c =>
      Sync[F].blocking(c.close(CloseOptions.timeout(cs.closeTimeout.toJava))))

}
