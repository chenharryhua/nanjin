package com.github.chenharryhua.nanjin.kafka
import cats.implicits._
import io.circe.generic.auto._
import cats.derived.auto.show._
class TopicNameSyntaxTest {
  val topic1 = ctx.topic[KJson[Key], Payment]("topic1")
  val topic2 = ctx.topic[Key, KJson[Payment]]("topic2")
  val topic3 = ctx.topic[Int, Int]("topic3")
  val tooic4 = ctx.topic(TopicDef[Int, KJson[Payment]]("topic4"))
  val topic5 = ctx.topic(TopicDef[Int, Payment]("topic5"))
  val topic6 = TopicDef[Int,Int]("topic6").in(ctx)
}
