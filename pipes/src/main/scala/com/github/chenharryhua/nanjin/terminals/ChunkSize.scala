package com.github.chenharryhua.nanjin.terminals

private opaque type ChunkSize = Int
private object ChunkSize:
  def apply(chunkSize: Int): ChunkSize = {
    require(chunkSize > 0, s"ChunkSize must be greater than zero, but was $chunkSize")
    chunkSize
  }

  extension (cs: ChunkSize) inline def value: Int = cs
end ChunkSize
