package com.github.chenharryhua.nanjin.common

import cats.Show
import cats.implicits.catsSyntaxEq
import cats.kernel.Eq
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.string.MatchesRegex
import io.circe.{Decoder, Encoder}

object kafka {
  type TopicNameC = String Refined MatchesRegex["""^[a-zA-Z0-9_.\-]+$"""]

  final class TopicName private (val value: String) extends Serializable
  object TopicName extends RefinedTypeOps[TopicNameC, String] {
    def apply(tnc: TopicNameC): TopicName = new TopicName(tnc.value)
    def unsafe(str: String): TopicName    = apply(unsafeFrom(str))

    implicit val showTopicName: Show[TopicName] = tn => s"TopicName(value=${tn.value})"
    implicit val eqTopicName: Eq[TopicName]     = Eq.instance((a, b) => a.value === b.value)

    implicit val encodeTopicName: Encoder[TopicName] = Encoder.encodeString.contramap(_.value)
    implicit val decodeTopicName: Decoder[TopicName] = Decoder.decodeString.emap(from(_).map(apply))
  }
}
