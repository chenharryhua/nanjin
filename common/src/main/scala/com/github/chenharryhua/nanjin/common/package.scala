package com.github.chenharryhua.nanjin

import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.string.MatchesRegex

package object common {
  type EmailAddr = Refined[String, MatchesRegex["""^\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$"""]]
  object EmailAddr extends RefinedTypeOps[EmailAddr, String] with CatsRefinedTypeOpsSyntax

  // number of records
  type ChunkSize = Refined[Int, Positive]
  object ChunkSize extends RefinedTypeOps[ChunkSize, Int] with CatsRefinedTypeOpsSyntax
}
