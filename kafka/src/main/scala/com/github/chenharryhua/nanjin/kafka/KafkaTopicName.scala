package com.github.chenharryhua.nanjin.kafka
import contextual._

import scala.util.matching.Regex

final case class KafkaTopicName(value: String) extends AnyVal {
  def keySchemaLoc: String   = s"$value-key"
  def valueSchemaLoc: String = s"$value-value"

  def in[K: ctx.SerdeOf, V: ctx.SerdeOf](ctx: KafkaContext): KafkaTopic[K, V] =
    ctx.topic[K, V](this)
}

object KafkaTopicNameInterpolator extends Verifier[KafkaTopicName] {
  override def check(string: String): Either[(Int, String), KafkaTopicName] = {
    val topicConstraint: Regex = "^[a-zA-Z0-9_.-]+$".r
    topicConstraint.findFirstMatchIn(string) match {
      case None    => Left((0, "topic name is invalid"))
      case Some(_) => Right(KafkaTopicName(string))
    }
  }
}

object KafkaTopicName {
  implicit class KafkaTopicNameStringContext(sc: StringContext) {
    val topic = Prefix(KafkaTopicNameInterpolator, sc)
  }
}
