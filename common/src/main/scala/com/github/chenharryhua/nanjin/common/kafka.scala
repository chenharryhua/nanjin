package com.github.chenharryhua.nanjin.common

import cats.implicits.toBifunctorOps
import cats.{Order, Show}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.refineV
import eu.timepit.refined.string.MatchesRegex
import io.circe.{Decoder, Encoder}

object kafka {
  private type MR = MatchesRegex["""^[a-zA-Z0-9_.\-]+$"""]

  type TopicNameL = String Refined MR

  final class TopicName private (val value: String) extends Serializable {
    override val toString: String = value
  }

  object TopicName {
    def apply(tnc: TopicNameL): TopicName = new TopicName(tnc.value)

    private def trans(str: String): Either[String, TopicName] = refineV[MR](str).map(apply)

    def from(str: String): Either[Exception, TopicName] = trans(str).leftMap(new Exception(_))

    def unsafeFrom(str: String): TopicName = from(str) match {
      case Left(value)  => throw value
      case Right(value) => value
    }

    implicit val showTopicName: Show[TopicName] = Show.fromToString

    implicit val orderingTopicName: Ordering[TopicName] = Ordering.by(_.value)
    implicit val orderTopicName: Order[TopicName] = Order.fromOrdering[TopicName]

    implicit val encodeTopicName: Encoder[TopicName] = Encoder.encodeString.contramap(_.value)
    implicit val decodeTopicName: Decoder[TopicName] = Decoder.decodeString.emap(trans)
  }
}
