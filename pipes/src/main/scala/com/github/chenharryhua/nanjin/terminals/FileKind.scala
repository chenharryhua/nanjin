package com.github.chenharryhua.nanjin.terminals

import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.datetime.codec
import io.circe.generic.JsonCodec

import java.time.format.DateTimeFormatter

@JsonCodec
sealed abstract class FileKind(val fileFormat: FileFormat, val compression: Compression) {
  private val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("HHmmss")

  final val fileName: String = compression.fileName(fileFormat)
  final def fileName(tick: Tick): String = {
    val seqId: String = tick.sequenceId.toString.take(5)
    val time: String = fmt.format(tick.zonedWakeup.toLocalTime)
    f"$seqId-${tick.index}%04d-$time.$fileName"
  }

  final def ymdFileName(tick: Tick): String = {
    val ymd = codec.year_month_day(tick.zonedWakeup.toLocalDate)
    s"$ymd/${fileName(tick)}"
  }
}

final case class AvroFile(override val compression: AvroCompression)
    extends FileKind(FileFormat.Avro, compression)

object AvroFile {
  def apply(f: AvroCompression.type => AvroCompression): AvroFile =
    AvroFile(f(AvroCompression))
}

final case class CirceFile(override val compression: CirceCompression)
    extends FileKind(FileFormat.Circe, compression)

object CirceFile {
  def apply(f: CirceCompression.type => CirceCompression): CirceFile =
    CirceFile(f(CirceCompression))
}

final case class BinAvroFile(override val compression: BinaryAvroCompression)
    extends FileKind(FileFormat.BinaryAvro, compression)

object BinAvroFile {
  def apply(f: BinaryAvroCompression.type => BinaryAvroCompression): BinAvroFile =
    BinAvroFile(f(BinaryAvroCompression))
}

final case class JacksonFile(override val compression: JacksonCompression)
    extends FileKind(FileFormat.Jackson, compression)

object JacksonFile {
  def apply(f: JacksonCompression.type => JacksonCompression): JacksonFile =
    JacksonFile(f(JacksonCompression))
}

final case class KantanFile(override val compression: KantanCompression)
    extends FileKind(FileFormat.Kantan, compression)

object KantanFile {
  def apply(f: KantanCompression.type => KantanCompression): KantanFile =
    KantanFile(f(KantanCompression))
}

final case class ParquetFile(override val compression: ParquetCompression)
    extends FileKind(FileFormat.Parquet, compression)

object ParquetFile {
  def apply(f: ParquetCompression.type => ParquetCompression): ParquetFile =
    ParquetFile(f(ParquetCompression))
}

final case class ProtobufFile(override val compression: ProtobufCompression)
    extends FileKind(FileFormat.ProtoBuf, compression)

object ProtobufFile {
  def apply(f: ProtobufCompression.type => ProtobufCompression): ProtobufFile =
    ProtobufFile(f(ProtobufCompression))
}

final case class TextFile(override val compression: TextCompression)
    extends FileKind(FileFormat.Text, compression)

object TextFile {
  def apply(f: TextCompression.type => TextCompression): TextFile =
    TextFile(f(TextCompression))
}
