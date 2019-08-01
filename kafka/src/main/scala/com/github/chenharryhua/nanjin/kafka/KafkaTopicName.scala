package com.github.chenharryhua.nanjin.kafka
import contextual._
import shapeless.Witness

final case class KafkaTopicName[K,V](value: String) extends AnyVal {
  def keySchemaLoc: String   = s"$value-key"
  def valueSchemaLoc: String = s"$value-value"
}
/*
object KafkaTopicNameInterpolator extends Verifier[KafkaTopicName] {
  import eu.timepit.refined.api.Refined
  import eu.timepit.refined.auto._
  import eu.timepit.refined.string._

  val topicConstraint: String Refined Regex = "^[a-zA-Z0-9_.-]+$"
  type TopicNameConstraint = String Refined MatchesRegex[Witness.`"^[a-zA-Z0-9_.-]+$"`.T]

  override def check(string: String): Either[(Int, String), KafkaTopicName] = {
    topicConstraint.value.r.findFirstMatchIn(string) match {
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
*/