package com.github.chenharryhua.nanjin.common

import cats.{Order, Show}
import io.circe.{Decoder, Encoder}

opaque type ChunkSize = Int
object ChunkSize:
  def apply(value: Int): ChunkSize = value
  extension (cs: ChunkSize)
    inline def value: Int = cs
    def -(other: ChunkSize): Int = cs - other.value
    def +(other: ChunkSize): Int = cs + other.value

  given Show[ChunkSize] = _.value.toString
  given Encoder[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Encoder]
  given Decoder[ChunkSize] = OpaqueLift.lift[ChunkSize, Int, Decoder]
  given Ordering[ChunkSize] = Ordering.by(_.value)
  given Order[ChunkSize] = Order.fromOrdering
end ChunkSize
