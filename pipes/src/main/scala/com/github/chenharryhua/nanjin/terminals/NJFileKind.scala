package com.github.chenharryhua.nanjin.terminals

import com.github.chenharryhua.nanjin.common.time.Tick
import io.circe.generic.JsonCodec

@JsonCodec
sealed abstract class NJFileKind(format: NJFileFormat, compression: NJCompression) {

  final val fileName: PathSegment           = PathSegment.unsafe(compression.fileName(format))
  final def rotate(tick: Tick): PathSegment = PathSegment.unsafe(f"${tick.index}%09d.${fileName.value}")

  final override val toString: String = fileName.value
}

final case class AvroFile(compression: AvroCompression) extends NJFileKind(NJFileFormat.Avro, compression)
final case class CirceFile(compression: CirceCompression) extends NJFileKind(NJFileFormat.Circe, compression)

final case class BinAvroFile(compression: BinaryAvroCompression)
    extends NJFileKind(NJFileFormat.BinaryAvro, compression)

final case class JacksonFile(compression: JacksonCompression)
    extends NJFileKind(NJFileFormat.Jackson, compression)

final case class KantanFile(compression: KantanCompression)
    extends NJFileKind(NJFileFormat.Kantan, compression)

final case class ParquetFile(compression: ParquetCompression)
    extends NJFileKind(NJFileFormat.Parquet, compression)
