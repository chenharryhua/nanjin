package com.github.chenharryhua.nanjin.kafka

import com.github.chenharryhua.nanjin.kafka.TopicDef._
import io.circe.generic.auto._

class TopicNameSyntaxTest {

  val tooic4 = ctx.topic(TopicDef[Int, KJson[Payment]]("topic4"))
  val topic5 = ctx.topic(TopicDef[Int, KAvro[Payment]]("topic5"))
}
