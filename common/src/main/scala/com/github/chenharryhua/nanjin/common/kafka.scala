package com.github.chenharryhua.nanjin.common

import cats.{Order, Show}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.MatchesRegex
import io.circe.Codec

object kafka {
  private type MR = MatchesRegex["""^[a-zA-Z0-9_.\-]+$"""]

  type TopicNameL = String Refined MR

  final case class TopicName(name: String) derives Codec.AsObject {
    val value: String = name
    override val toString: String = name
  }

  object TopicName {

    implicit val showTopicName: Show[TopicName] = Show.fromToString

    implicit val orderingTopicName: Ordering[TopicName] = Ordering.by(_.name)
    implicit val orderTopicName: Order[TopicName] = Order.fromOrdering[TopicName]

  }
}
