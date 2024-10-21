package com.github.chenharryhua.nanjin

import cats.{Order, Show}
import io.circe.{Decoder, Encoder, HCursor, Json}
import org.apache.kafka.common.TopicPartition

package object kafka {
  implicit val orderingTopicPartition: Ordering[TopicPartition] =
    Ordering.by(tp => (tp.topic(), tp.partition()))

  implicit val orderTopicPartition: Order[TopicPartition] =
    Order.fromOrdering[TopicPartition]

  implicit val showTopicPartition: Show[TopicPartition] =
    Show.fromToString[TopicPartition]

  implicit val encoderTopicPartition: Encoder[TopicPartition] =
    (a: TopicPartition) =>
      Json.obj("topic" -> Json.fromString(a.topic()), "partition" -> Json.fromInt(a.partition()))

  implicit val decoderTopicPartition: Decoder[TopicPartition] =
    (c: HCursor) =>
      for {
        topic <- c.downField("topic").as[String]
        partition <- c.downField("partition").as[Int]
      } yield new TopicPartition(topic, partition)
}
