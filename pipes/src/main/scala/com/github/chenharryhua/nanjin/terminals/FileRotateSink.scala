package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Hotswap
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.github.chenharryhua.nanjin.common.chrono.TickedValue
import fs2.{Chunk, Pipe, Pull, Stream}
import io.circe.Json
import io.circe.jackson.circeToJackson
import io.lemonlabs.uri.Url
import kantan.csv.CsvConfiguration
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.avro.AvroParquetWriter.Builder
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopOutputFile

final class FileRotateSink[F[_]: Async] private (
  configuration: Configuration,
  paths: Stream[F, TickedValue[Path]]) {
  // avro
  def avro(compression: AvroCompression): Pipe[F, Chunk[GenericRecord], TickedValue[Int]] = {

    (ss: Stream[F, Chunk[GenericRecord]]) =>
      getSchema(ss).stream.flatMap { schema =>
        def get_writer(url: Path): Resource[F, HadoopWriter[F, GenericRecord]] =
          HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, url)

        periodically.persist(ss, paths, get_writer)
      }
  }

  def avro(f: AvroCompression.type => AvroCompression): Pipe[F, Chunk[GenericRecord], TickedValue[Int]] =
    avro(f(AvroCompression))

  val avro: Pipe[F, Chunk[GenericRecord], TickedValue[Int]] =
    avro(AvroCompression.Uncompressed)

  // binary avro
  val binAvro: Pipe[F, Chunk[GenericRecord], TickedValue[Int]] = { (ss: Stream[F, Chunk[GenericRecord]]) =>
    getSchema(ss).stream.flatMap { schema =>
      def get_writer(url: Path): Resource[F, HadoopWriter[F, GenericRecord]] =
        HadoopWriter.binAvroR[F](configuration, schema, url)
      periodically.persist(ss, paths, get_writer)
    }
  }

  // jackson json
  val jackson: Pipe[F, Chunk[GenericRecord], TickedValue[Int]] = { (ss: Stream[F, Chunk[GenericRecord]]) =>
    getSchema(ss).stream.flatMap { schema =>
      def get_writer(url: Path): Resource[F, HadoopWriter[F, GenericRecord]] =
        HadoopWriter.jacksonR[F](configuration, schema, url)
      periodically.persist(ss, paths, get_writer)
    }
  }

  // parquet
  def parquet(f: Endo[Builder[GenericRecord]]): Pipe[F, Chunk[GenericRecord], TickedValue[Int]] = {
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      getSchema(ss).stream.flatMap { schema =>
        val writeBuilder = Reader((path: Path) =>
          AvroParquetWriter
            .builder[GenericRecord](HadoopOutputFile.fromPath(path, configuration))
            .withDataModel(GenericData.get())
            .withConf(configuration)
            .withSchema(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)).map(f)

        def get_writer(url: Path): Resource[F, HadoopWriter[F, GenericRecord]] =
          HadoopWriter.parquetR[F](writeBuilder, url)

        periodically.persist(ss, paths, get_writer)
      }
  }

  val parquet: Pipe[F, Chunk[GenericRecord], TickedValue[Int]] =
    parquet(identity)

  // bytes
  val bytes: Pipe[F, Chunk[Byte], TickedValue[Int]] = {
    def get_writer(url: Path): Resource[F, HadoopWriter[F, Byte]] =
      HadoopWriter.byteR[F](configuration, url)

    (ss: Stream[F, Chunk[Byte]]) => periodically.persist(ss, paths, get_writer)
  }

  // circe json
  val circe: Pipe[F, Chunk[Json], TickedValue[Int]] = {
    val mapper: ObjectMapper = new ObjectMapper() // create ObjectMapper is expensive

    def get_writer(url: Path): Resource[F, HadoopWriter[F, JsonNode]] =
      HadoopWriter.jsonNodeR[F](configuration, url, mapper)

    (ss: Stream[F, Chunk[Json]]) => periodically.persist(ss.map(_.map(circeToJackson)), paths, get_writer)
  }

  // kantan csv
  def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Chunk[Seq[String]], TickedValue[Int]] = {
    def get_writer(url: Path): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.csvStringR[F](configuration, url)

    (ss: Stream[F, Chunk[Seq[String]]]) =>
      val encodedSrc: Stream[F, Chunk[String]] = ss.map(_.map(csvRow(csvConfiguration)))
      if (csvConfiguration.hasHeader) {
        val header: Chunk[String] = csvHeader(csvConfiguration)
        paths.pull.uncons1.flatMap {
          case Some((head, tail)) =>
            Stream
              .resource(Hotswap(get_writer(head.value)))
              .flatMap { case (hotswap, writer) =>
                Stream.eval(writer.write(header)) >>
                  periodically
                    .persistCsvWithHeader[F, Path](
                      head.tick,
                      get_writer,
                      hotswap,
                      writer,
                      encodedSrc.map(Left(_)).mergeHaltBoth(tail.map(Right(_))),
                      header
                    )
                    .stream
              }
              .pull
              .echo
          case None => Pull.done
        }.stream
      } else periodically.persist(encodedSrc, paths, get_writer)
  }

  def kantan(f: Endo[CsvConfiguration]): Pipe[F, Chunk[Seq[String]], TickedValue[Int]] =
    kantan(f(CsvConfiguration.rfc))

  val kantan: Pipe[F, Chunk[Seq[String]], TickedValue[Int]] =
    kantan(CsvConfiguration.rfc)

  // text
  val text: Pipe[F, Chunk[String], TickedValue[Int]] = {
    def get_writer(url: Path): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR(configuration, url)

    (ss: Stream[F, Chunk[String]]) => periodically.persist(ss, paths, get_writer)
  }
}

object FileRotateSink {
  def apply[F[_]: Async](
    configuration: Configuration,
    paths: Stream[F, TickedValue[Url]]): FileRotateSink[F] =
    new FileRotateSink(configuration, paths.map(tv => TickedValue(tv.tick, toHadoopPath(tv.value))))
}
