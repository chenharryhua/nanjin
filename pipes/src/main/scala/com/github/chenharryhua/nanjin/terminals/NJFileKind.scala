package com.github.chenharryhua.nanjin.terminals

import com.github.chenharryhua.nanjin.datetime.codec
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import io.circe.generic.JsonCodec

import java.time.ZoneId

@JsonCodec
sealed abstract class NJFileKind(val fileFormat: NJFileFormat, val compression: NJCompression) {
  final val fileName: String             = compression.fileName(fileFormat)
  final def fileName(tick: Tick): String = f"${tick.streamId.toString.take(5)}-${tick.index}%06d.$fileName"

  final def fileName(zoneId: ZoneId, tick: Tick): String = {
    val ymd = codec.year_month_day(tick.timestamp.atZone(zoneId).toLocalDate)
    s"$ymd/${fileName(tick)}"
  }
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

final case class ProtobufFile(override val compression: ProtobufCompression)
    extends NJFileKind(NJFileFormat.ProtoBuf, compression)

final case class TextFile(override val compression: TextCompression)
    extends NJFileKind(NJFileFormat.Text, compression)
