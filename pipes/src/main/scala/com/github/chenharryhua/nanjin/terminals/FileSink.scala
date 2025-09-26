package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.Sync
import cats.implicits.toFunctorOps
import com.fasterxml.jackson.databind.JsonNode
import fs2.{Pipe, Pull, Stream}
import io.circe.Json
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile
import scalapb.GeneratedMessage

sealed trait FileSink[F[_]] {

  /** [[https://avro.apache.org]]
    */
  def avro(compression: AvroCompression): Pipe[F, GenericRecord, Int]

  /** [[https://avro.apache.org]]
    */
  def avro(f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, Int]

  /** [[https://avro.apache.org]]
    */
  def avro: Pipe[F, GenericRecord, Int]

  /** [[https://avro.apache.org]]
    */
  def binAvro: Pipe[F, GenericRecord, Int]

  /** [[https://github.com/FasterXML/jackson]]
    */
  def jackson: Pipe[F, GenericRecord, Int]

  /** [[https://parquet.apache.org]]
    */
  def parquet(f: Endo[AvroParquetWriter.Builder[GenericRecord]]): Pipe[F, GenericRecord, Int]

  /** [[https://parquet.apache.org]]
    */
  def parquet: Pipe[F, GenericRecord, Int]

  // bytes
  def bytes: Pipe[F, Byte, Int]

  /** [[https://github.com/circe/circe]]
    */
  def circe: Pipe[F, Json, Int]

  /** [[https://nrinaudo.github.io/kantan.csv]]
    */
  def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Seq[String], Int]

  /** [[https://nrinaudo.github.io/kantan.csv]]
    */
  def kantan(f: Endo[CsvConfiguration]): Pipe[F, Seq[String], Int]

  /** [[https://nrinaudo.github.io/kantan.csv]]
    */
  def kantan: Pipe[F, Seq[String], Int]

  // text
  def text: Pipe[F, String, Int]

  /** Any proto in serialized form must be <2GiB, as that is the maximum size supported by all
    * implementations. It’s recommended to bound request and response sizes.
    *
    * https://protobuf.dev/programming-guides/proto-limits/#total
    */
  def protobuf: Pipe[F, GeneratedMessage, Int]

  /** [[https://github.com/FasterXML/jackson-databind]]
    * @return
    */
  def jsonNode: Pipe[F, JsonNode, Int]
}

final private class FileSinkImpl[F[_]: Sync](configuration: Configuration, url: Url) extends FileSink[F] {

  override def avro(compression: AvroCompression): Pipe[F, GenericRecord, Int] = {
    (ss: Stream[F, GenericRecord]) =>
      ss.pull.stepLeg.flatMap {
        case Some(leg) =>
          val schema = leg.head(0).getSchema
          Stream
            .resource(HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, url))
            .flatMap(w => leg.stream.cons(leg.head).chunks.evalMap(c => w.write(c).as(c.size)))
            .pull
            .echo
        case None => Pull.done
      }.stream
  }

  override def avro(f: AvroCompression.type => AvroCompression): Pipe[F, GenericRecord, Int] =
    avro(f(AvroCompression))

  override val avro: Pipe[F, GenericRecord, Int] =
    avro(AvroCompression.Uncompressed)

  override val binAvro: Pipe[F, GenericRecord, Int] = { (ss: Stream[F, GenericRecord]) =>
    ss.pull.stepLeg.flatMap {
      case Some(leg) =>
        val schema = leg.head(0).getSchema
        Stream
          .resource(HadoopWriter.binAvroR[F](configuration, schema, url))
          .flatMap(w => leg.stream.cons(leg.head).chunks.evalMap(c => w.write(c).as(c.size)))
          .pull
          .echo
      case None => Pull.done
    }.stream
  }

  override val jackson: Pipe[F, GenericRecord, Int] = { (ss: Stream[F, GenericRecord]) =>
    ss.pull.stepLeg.flatMap {
      case Some(leg) =>
        val schema = leg.head(0).getSchema
        Stream
          .resource(HadoopWriter.jacksonR[F](configuration, schema, url))
          .flatMap(w => leg.stream.cons(leg.head).chunks.evalMap(c => w.write(c).as(c.size)))
          .pull
          .echo
      case None => Pull.done
    }.stream
  }

  override def parquet(f: Endo[AvroParquetWriter.Builder[GenericRecord]]): Pipe[F, GenericRecord, Int] = {
    (ss: Stream[F, GenericRecord]) =>
      ss.pull.stepLeg.flatMap {
        case Some(leg) =>
          val schema = leg.head(0).getSchema
          val writeBuilder = Reader((path: Path) =>
            AvroParquetWriter
              .builder[GenericRecord](HadoopOutputFile.fromPath(path, configuration))
              .withDataModel(GenericData.get())
              .withConf(configuration)
              .withSchema(schema)
              .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
              .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)).map(f)

          Stream
            .resource(HadoopWriter.parquetR[F](writeBuilder, url))
            .flatMap(w => leg.stream.cons(leg.head).chunks.evalMap(c => w.write(c).as(c.size)))
            .pull
            .echo
        case None => Pull.done
      }.stream
  }

  override val parquet: Pipe[F, GenericRecord, Int] =
    parquet(identity)

  override val bytes: Pipe[F, Byte, Int] = { (ss: Stream[F, Byte]) =>
    Stream
      .resource(HadoopWriter.byteR[F](configuration, url))
      .flatMap(w => ss.chunks.evalMap(c => w.write(c).as(c.size)))
  }

  override val circe: Pipe[F, Json, Int] = { (ss: Stream[F, Json]) =>
    Stream
      .resource(HadoopWriter.circeR[F](configuration, url))
      .flatMap(w => ss.chunks.evalMap(c => w.write(c).as(c.size)))
  }

  override def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Seq[String], Int] = {
    (ss: Stream[F, Seq[String]]) =>
      Stream
        .resource(
          HadoopWriter.csvStringR[F](configuration, url).evalTap(_.write(csvHeader(csvConfiguration))))
        .flatMap { w =>
          ss.map(csvRow(csvConfiguration)).chunks.evalMap(c => w.write(c).as(c.size))
        }
  }

  override def kantan(f: Endo[CsvConfiguration]): Pipe[F, Seq[String], Int] =
    kantan(f(CsvConfiguration.rfc))

  override val kantan: Pipe[F, Seq[String], Int] =
    kantan(CsvConfiguration.rfc)

  override val text: Pipe[F, String, Int] = { (ss: Stream[F, String]) =>
    Stream
      .resource(HadoopWriter.stringR[F](configuration, url))
      .flatMap(w => ss.chunks.evalMap(c => w.write(c).as(c.size)))
  }

  override val protobuf: Pipe[F, GeneratedMessage, Int] = { (ss: Stream[F, GeneratedMessage]) =>
    Stream.resource(HadoopWriter.protobufR(configuration, url)).flatMap { w =>
      ss.chunks.evalMap(c => w.write(c).as(c.size))
    }
  }

  override def jsonNode: Pipe[F, JsonNode, Int] = { (ss: Stream[F, JsonNode]) =>
    Stream.resource(HadoopWriter.jsonNodeR(configuration, url)).flatMap { w =>
      ss.chunks.evalMap(c => w.write(c).as(c.size))
    }
  }
}
