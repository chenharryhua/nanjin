package com.github.chenharryhua.nanjin.common

import cats.Show
import cats.implicits.catsSyntaxEq
import cats.kernel.Eq
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

    def unsafe(str: String): TopicName = trans(str) match {
      case Left(value)  => throw new Exception(value)
      case Right(value) => value
    }

    implicit val showTopicName: Show[TopicName] = tn => s"TopicName(value=${tn.value})"
    implicit val eqTopicName: Eq[TopicName]     = Eq.instance((a, b) => a.value === b.value)

    implicit val encodeTopicName: Encoder[TopicName] = Encoder.encodeString.contramap(_.value)
    implicit val decodeTopicName: Decoder[TopicName] = Decoder.decodeString.emap(trans)
  }
}
