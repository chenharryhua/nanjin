package com.github.chenharryhua.nanjin

import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.boolean.And
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.collection.NonEmpty
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.string.{MatchesRegex, Trimmed, Uri}

package object common {
  type EmailAddr = Refined[String, MatchesRegex["""^\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$"""]]
  object EmailAddr extends RefinedTypeOps[EmailAddr, String] with CatsRefinedTypeOpsSyntax

  // number of records
  type ChunkSize = Refined[Int, Positive]
  object ChunkSize extends RefinedTypeOps[ChunkSize, Int] with CatsRefinedTypeOpsSyntax

  type PathSegment = Refined[String, MatchesRegex["""^[a-zA-Z0-9_.=\-]+$"""]]
  object PathSegment extends RefinedTypeOps[PathSegment, String] with CatsRefinedTypeOpsSyntax

  type PathRoot = Refined[String, Uri]
  object PathRoot extends RefinedTypeOps[PathRoot, String] with CatsRefinedTypeOpsSyntax

  type NameConstraint = NonEmpty And Trimmed
}
