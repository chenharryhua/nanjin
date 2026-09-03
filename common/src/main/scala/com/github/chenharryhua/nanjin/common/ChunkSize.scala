package com.github.chenharryhua.nanjin.common

import cats.{Order, Show}
import io.circe.{Decoder, Encoder}

opaque type ChunkSize = Int
object ChunkSize:
  def apply(chunkSize: Int): ChunkSize = {
    require(chunkSize > 0, s"ChunkSize must be greater than zero, but was $chunkSize")
    chunkSize
  }

  extension (cs: ChunkSize) inline def value: Int = cs

  given Show[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Show]
  given Encoder[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Encoder]
  given Decoder[ChunkSize] = Decoder.decodeInt.emap { chunkSize =>
    Either.cond(chunkSize > 0, chunkSize, s"ChunkSize must be greater than zero, but was $chunkSize")
  }
  given Ordering[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Ordering]
  given Order[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Order]

  given Conversion[Int, ChunkSize] with
    override def apply(cs: Int): ChunkSize = ChunkSize.apply(cs)
end ChunkSize
