package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.syntax.apply.given
import cats.syntax.eq.given
import io.circe.Decoder.Result
import io.circe.syntax.EncoderOps
import io.circe.{Codec, DecodingFailure, HCursor, Json}
import io.lemonlabs.uri.{Uri, Url}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.avro.AvroParquetWriter.Builder
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile

import java.net.URI
import java.time.LocalDate
import scala.annotation.tailrec
import scala.util.Try

given codecUrl: Codec[Url] = new Codec[Url] {

  /** scala-uri parses any URI but only some of them are URLs: `Urn.toUrl` throws `UriConversionException`,
    * which circe does not catch, so a payload like `urn:isbn:0451450523` has to be turned into a
    * `DecodingFailure` here rather than escaping the decoder as a thrown exception.
    */
  override def apply(c: HCursor): Result[Url] =
    c.as[URI].flatMap { (uri: URI) =>
      Try(Uri(uri).toUrl).toEither.left.map(ex =>
        DecodingFailure(s"not a url: $uri. ${ex.getMessage}", c.history))
    }

  override def apply(a: Url): Json = a.toJavaURI.asJson
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
        partitionPath.year(head) match {
          case Some(year) =>
            next match {
              case month :: day :: _ =>
                (partitionPath.month(month), partitionPath.day(day)).flatMapN { case (m, d) =>
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

private def defaultParquetWriteBuilder(
  configuration: Configuration,
  schema: Schema,
  f: Endo[Builder[GenericRecord]]): Reader[Url, Builder[GenericRecord]] =
  Reader((url: Url) =>
    AvroParquetWriter
      .builder[GenericRecord](HadoopOutputFile.fromPath(toHadoopPath(url), configuration))
      .withDataModel(GenericData.get())
      .withConf(configuration)
      .withSchema(schema)
      .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)).map(f)
