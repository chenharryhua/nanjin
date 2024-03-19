package com.github.chenharryhua.nanjin.terminals

import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.datetime.codec
import io.circe.generic.JsonCodec

@JsonCodec
sealed abstract class NJFileKind(val fileFormat: NJFileFormat, val compression: NJCompression) {
  final val fileName: String             = compression.fileName(fileFormat)
  final def fileName(tick: Tick): String = f"${tick.sequenceId.toString.take(5)}-${tick.index}%06d.$fileName"

  final def ymdFileName(tick: Tick): String = {
    val ymd = codec.year_month_day(tick.zonedWakeup.toLocalDate)
    s"$ymd/${fileName(tick)}"
  }
}

final case class AvroFile(override val compression: AvroCompression)
    extends NJFileKind(NJFileFormat.Avro, compression)

object AvroFile {
  def apply(f: AvroCompression.type => AvroCompression): AvroFile =
    AvroFile(f(AvroCompression))
}

final case class CirceFile(override val compression: CirceCompression)
    extends NJFileKind(NJFileFormat.Circe, compression)

object CirceFile {
  def apply(f: CirceCompression.type => CirceCompression): CirceFile =
    CirceFile(f(CirceCompression))
}

final case class BinAvroFile(override val compression: BinaryAvroCompression)
    extends NJFileKind(NJFileFormat.BinaryAvro, compression)

object BinAvroFile {
  def apply(f: BinaryAvroCompression.type => BinaryAvroCompression): BinAvroFile =
    BinAvroFile(f(BinaryAvroCompression))
}

final case class JacksonFile(override val compression: JacksonCompression)
    extends NJFileKind(NJFileFormat.Jackson, compression)

object JacksonFile {
  def apply(f: JacksonCompression.type => JacksonCompression): JacksonFile =
    JacksonFile(f(JacksonCompression))
}

final case class KantanFile(override val compression: KantanCompression)
    extends NJFileKind(NJFileFormat.Kantan, compression)

object KantanFile {
  def apply(f: KantanCompression.type => KantanCompression): KantanFile =
    KantanFile(f(KantanCompression))
}

final case class ParquetFile(override val compression: ParquetCompression)
    extends NJFileKind(NJFileFormat.Parquet, compression)

object ParquetFile {
  def apply(f: ParquetCompression.type => ParquetCompression): ParquetFile =
    ParquetFile(f(ParquetCompression))
}

final case class ProtobufFile(override val compression: ProtobufCompression)
    extends NJFileKind(NJFileFormat.ProtoBuf, compression)

object ProtobufFile {
  def apply(f: ProtobufCompression.type => ProtobufCompression): ProtobufFile =
    ProtobufFile(f(ProtobufCompression))
}

final case class TextFile(override val compression: TextCompression)
    extends NJFileKind(NJFileFormat.Text, compression)

object TextFile {
  def apply(f: TextCompression.type => TextCompression): TextFile =
    TextFile(f(TextCompression))
}
