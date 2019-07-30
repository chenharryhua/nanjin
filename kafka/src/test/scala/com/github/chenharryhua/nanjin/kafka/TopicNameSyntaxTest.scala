package com.github.chenharryhua.nanjin.kafka

import cats.effect.IO
import KafkaTopicName._

class TopicNameSyntaxTest {
  val tn1 = KafkaTopicName("topic1").in[IO, Int, Int](ctx)
  val tn2 = topic"topic2".in[IO, Int, Int](ctx)
  val tn3 = topic"topic3".in[IO, Int, Int](ctx)
  val tn4 = ctx.topic[Int, Int]("topic4")
  val tn5 = ctx.topic[Int, Int](topic"topic5")
}
