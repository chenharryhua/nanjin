package com.github.chenharryhua.nanjin.common

import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.string.MatchesRegex

object kafka {
  type TopicName = String Refined MatchesRegex["""^[a-zA-Z0-9_.\-]+$"""]

  object TopicName extends RefinedTypeOps[TopicName, String] with CatsRefinedTypeOpsSyntax

  type StoreName = TopicName

  object StoreName extends RefinedTypeOps[TopicName, String] with CatsRefinedTypeOpsSyntax

}
