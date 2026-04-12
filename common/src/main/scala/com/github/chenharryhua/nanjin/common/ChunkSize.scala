package com.github.chenharryhua.nanjin.common

import cats.{Order, Show}
import io.circe.{Decoder, Encoder}

opaque type ChunkSize = Int
object ChunkSize:
  def apply(cs: Int): ChunkSize = cs
  extension (cs: ChunkSize) inline def value: Int = cs

  given Show[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Show]
  given Encoder[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Encoder]
  given Decoder[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Decoder]
  given Ordering[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Ordering]
  given Order[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Order]

  given Conversion[Int, ChunkSize] with
    override def apply(cs: Int): ChunkSize = cs
end ChunkSize
