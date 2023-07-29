package com.github.chenharryhua.nanjin.terminals

import com.github.chenharryhua.nanjin.common.time.Tick
import io.circe.generic.JsonCodec

@JsonCodec
sealed abstract class NJFileKind(val fileFormat: NJFileFormat, val compression: NJCompression) {
  final val fileName: String           = compression.fileName(fileFormat)
  final def rotate(tick: Tick): String = f"${tick.index}%09d.$fileName"
}

final case class AvroFile(override val compression: AvroCompression)
    extends NJFileKind(NJFileFormat.Avro, compression)

final case class CirceFile(override val compression: CirceCompression)
    extends NJFileKind(NJFileFormat.Circe, compression)

final case class BinAvroFile(override val compression: BinaryAvroCompression)
    extends NJFileKind(NJFileFormat.BinaryAvro, compression)

final case class JacksonFile(override val compression: JacksonCompression)
    extends NJFileKind(NJFileFormat.Jackson, compression)

final case class KantanFile(override val compression: KantanCompression)
    extends NJFileKind(NJFileFormat.Kantan, compression)

final case class ParquetFile(override val compression: ParquetCompression)
    extends NJFileKind(NJFileFormat.Parquet, compression)
