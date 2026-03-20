package com.github.chenharryhua.nanjin.common

import cats.{Order, Show}
import io.circe.{Decoder, Encoder}
import io.github.iltotore.iron.RefinedType
import io.github.iltotore.iron.constraint.numeric.Positive

type ChunkSize = ChunkSize.T
object ChunkSize extends RefinedType[Int, Positive] with IronRefined.PlusConversion[Int, Positive]:
  given Show[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Show]
  given Encoder[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Encoder]
  given Decoder[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Decoder]
  given Ordering[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Ordering]
  given Order[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Order]
end ChunkSize
