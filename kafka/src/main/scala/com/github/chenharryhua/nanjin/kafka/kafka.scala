package com.github.chenharryhua.nanjin

import cats.implicits._
import eu.timepit.refined.W
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.string.MatchesRegex

package object kafka extends ShowKafkaMessage {

  type TopicName = String Refined MatchesRegex[W.`"^[a-zA-Z0-9_.-]+$"`.T]

  object TopicName extends RefinedTypeOps[TopicName, String] with CatsRefinedTypeOpsSyntax
}
