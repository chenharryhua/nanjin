package com.github.chenharryhua.nanjin

import cats.{Order, Show}
import fs2.kafka.{ConsumerSettings, Id}
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.kafka.common.TopicPartition

package object kafka {
  final val TOPIC: String = "topic"
  final val PARTITION: String = "partition"

  implicit val orderingTopicPartition: Ordering[TopicPartition] =
    Ordering.by(tp => (tp.topic(), tp.partition()))

  implicit val orderTopicPartition: Order[TopicPartition] =
    Order.fromOrdering[TopicPartition]

  implicit val showTopicPartition: Show[TopicPartition] =
    Show.fromToString[TopicPartition]

  implicit val encoderTopicPartition: Encoder[TopicPartition] =
    (a: TopicPartition) =>
      Json.obj(TOPIC -> Json.fromString(a.topic()), PARTITION -> Json.fromInt(a.partition()))

  implicit val decoderTopicPartition: Decoder[TopicPartition] =
    (c: HCursor) =>
      for {
        topic <- c.downField(TOPIC).as[String]
        partition <- c.downField(PARTITION).as[Int]
      } yield new TopicPartition(topic, partition)

  type PureConsumerSettings = ConsumerSettings[Id, Nothing, Nothing]
  val pureConsumerSettings: PureConsumerSettings = ConsumerSettings[Id, Nothing, Nothing](null, null)

}
