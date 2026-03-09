package com.github.chenharryhua.nanjin


package object common {
  // Simple email validation: checks general "local@domain.tld" format.
  // Does NOT cover all RFC 5322 edge cases (comments, quoted strings, IP literals, etc.)
  // Chosen for readability, maintainability, and practical usage in typical applications.
  type EmailAddr = String //Refined[String, MatchesRegex["""^\w+([-+.']\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*$"""]]
  //object EmailAddr extends RefinedTypeOps[EmailAddr, String] with CatsRefinedTypeOpsSyntax

  // number of records
  type ChunkSize = Int //Refined[Int, Positive]
  // object ChunkSize extends RefinedTypeOps[ChunkSize, Int] with CatsRefinedTypeOpsSyntax {
  //   def unsafeFromBytes(bytes: Information): ChunkSize = {
  //     val b = bytes.toBytes
  //     require(b >= 1, s"ChunkSize($b) must be positive")
  //     unsafeFrom(b.toInt)
  //   }
  //   @inline def apply(bufferSize: Information): ChunkSize = unsafeFromBytes(bufferSize)
  // }
}
