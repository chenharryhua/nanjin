package com.github.chenharryhua.nanjin

import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.numeric.Positive
import eu.timepit.refined.string.MatchesRegex
import squants.information.Information

package object common {
  type EmailAddr = Refined[String, MatchesRegex["""^\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$"""]]
  object EmailAddr extends RefinedTypeOps[EmailAddr, String] with CatsRefinedTypeOpsSyntax

  // number of records
  type ChunkSize = Refined[Int, Positive]
  object ChunkSize extends RefinedTypeOps[ChunkSize, Int] with CatsRefinedTypeOpsSyntax {
    def fromBytes(bytes: Information): ChunkSize = {
      val b = bytes.toBytes
      require(b >= 1 && b <= Int.MaxValue, s"ChunkSize($b) out of Int range")
      unsafeFrom(b.toInt)
    }
    def apply(bufferSize: Information): ChunkSize = fromBytes(bufferSize)
  }
}
