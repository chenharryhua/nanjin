package com.github.chenharryhua.nanjin

import cats.Endo
import cats.data.Reader
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import cats.syntax.eq.catsSyntaxEq
import com.github.chenharryhua.nanjin.datetime.codec
import eu.timepit.refined.api.{Refined, RefinedTypeOps}
import eu.timepit.refined.cats.CatsRefinedTypeOpsSyntax
import eu.timepit.refined.numeric.Interval.Closed
import fs2.Chunk
import io.circe.{Decoder, Encoder}
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration
import kantan.csv.CsvConfiguration.Header
import kantan.csv.engine.WriterEngine
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.avro.AvroParquetWriter.Builder
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile

import java.io.StringWriter
import java.time.LocalDate
import scala.annotation.tailrec
import scala.util.Try

package object terminals {

  implicit val encoderUrl: Encoder[Url] = Encoder.encodeString.contramap(_.toString())
  implicit val decoderUrl: Decoder[Url] = Decoder.decodeString.map(Url.parse(_))

  type NJCompressionLevel = Int Refined Closed[1, 9]
  object NJCompressionLevel extends RefinedTypeOps[NJCompressionLevel, Int] with CatsRefinedTypeOpsSyntax

  def csvRow(csvConfiguration: CsvConfiguration)(row: Seq[String]): String = {
    val sw = new StringWriter()
    WriterEngine.internalCsvWriterEngine.writerFor(sw, csvConfiguration).write(row).close()
    sw.toString
  }

  def headerWithCrlf(csvConfiguration: CsvConfiguration): Chunk[String] =
    csvConfiguration.header match {
      case Header.None             => Chunk.empty
      case Header.Implicit         => Chunk.singleton("no header was explicitly provided\r\n") // csv use CRLF
      case Header.Explicit(header) => Chunk.singleton(csvRow(csvConfiguration)(header))
    }

  def toHadoopPath(url: Url): Path =
    if (url.schemeOption.exists(_ === "s3")) {
      new Path(url.withScheme("s3a").normalize(removeEmptyPathParts = true).toJavaURI)
    } else
      new Path(url.normalize(removeEmptyPathParts = true).toJavaURI)

  /** extract LocalDate from a url
    * @return
    *   optional LocalDate
    */
  def extractDate(url: Url): Option[LocalDate] = {
    @tailrec
    def go(ps: List[String]): Option[LocalDate] =
      ps match {
        case head :: next =>
          codec.year(head) match {
            case Some(year) =>
              next match {
                case month :: day :: _ =>
                  (codec.month(month), codec.day(day)).flatMapN { case (m, d) =>
                    Try(LocalDate.of(year, m, d)).toOption
                  }
                case _ => None
              }
            case None => go(next)
          }
        case Nil => None
      }
    go(url.path.parts.toList)
  }

  def default_parquet_write_builder(
    configuration: Configuration,
    schema: Schema,
    f: Endo[Builder[GenericRecord]]): Reader[Url, Builder[GenericRecord]] = Reader((url: Url) =>
    AvroParquetWriter
      .builder[GenericRecord](HadoopOutputFile.fromPath(toHadoopPath(url), configuration))
      .withDataModel(GenericData.get())
      .withConf(configuration)
      .withSchema(schema)
      .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)).map(f)
}
