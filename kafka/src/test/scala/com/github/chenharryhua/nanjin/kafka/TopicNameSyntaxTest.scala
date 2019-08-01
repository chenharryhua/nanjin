package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.kafka.KafkaTopicName._
import io.circe.generic.auto._

class TopicNameSyntaxTest {

  val tooic4 = ctx.topic[Int, KJson[Payment]](KafkaTopicName("topic4"))
  val topic5 = ctx.topic[Int, KAvro[Payment]](KafkaTopicName("topic5"))
}
