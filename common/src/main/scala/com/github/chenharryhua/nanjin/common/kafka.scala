package com.github.chenharryhua.nanjin.common

import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.string.MatchesRegex

object kafka {
  type TopicNameC = String Refined MatchesRegex["""^[a-zA-Z0-9_.\-]+$"""]

  final class TopicName private (val value: String) extends Serializable
  object TopicName extends RefinedTypeOps[TopicNameC, String] with CatsRefinedTypeOpsSyntax {
    def apply(tnc: TopicNameC): TopicName = new TopicName(tnc.value)
    def unsafe(tn: String): TopicName     = apply(unsafeFrom(tn))
  }
}
