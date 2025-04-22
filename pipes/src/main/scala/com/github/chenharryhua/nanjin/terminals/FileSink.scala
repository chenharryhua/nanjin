package com.github.chenharryhua.nanjin.terminals

import cats.Endo
import cats.data.Reader
import cats.effect.kernel.{Resource, Sync}
import cats.implicits.toFunctorOps
import fs2.{Chunk, Pipe, Pull, Stream}
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

import java.io.OutputStream

final class FileSink[F[_]: Sync] private (configuration: Configuration, path: Path) {

  // avro
  def avro(compression: AvroCompression): Pipe[F, Chunk[GenericRecord], Int] = {
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      ss.pull.stepLeg.flatMap {
        case Some(leg) =>
          val schema = leg.head.flatMap(identity)(0).getSchema
          Stream
            .resource(HadoopWriter.avroR[F](compression.codecFactory, schema, configuration, path))
            .flatMap(w => (Stream.chunk(leg.head) ++ leg.stream).evalMap(c => w.write(c).as(c.size)))
            .pull
            .echo
        case None => Pull.done
      }.stream
  }

  def avro(f: AvroCompression.type => AvroCompression): Pipe[F, Chunk[GenericRecord], Int] =
    avro(f(AvroCompression))

  val avro: Pipe[F, Chunk[GenericRecord], Int] =
    avro(AvroCompression.Uncompressed)

  // binary avro
  val binAvro: Pipe[F, Chunk[GenericRecord], Int] = { (ss: Stream[F, Chunk[GenericRecord]]) =>
    ss.pull.stepLeg.flatMap {
      case Some(leg) =>
        val schema = leg.head.flatMap(identity)(0).getSchema
        Stream
          .resource(HadoopWriter.binAvroR[F](configuration, schema, path))
          .flatMap(w => (Stream.chunk(leg.head) ++ leg.stream).evalMap(c => w.write(c).as(c.size)))
          .pull
          .echo
      case None => Pull.done
    }.stream
  }

  // jackson json
  val jackson: Pipe[F, Chunk[GenericRecord], Int] = { (ss: Stream[F, Chunk[GenericRecord]]) =>
    ss.pull.stepLeg.flatMap {
      case Some(leg) =>
        val schema = leg.head.flatMap(identity)(0).getSchema
        Stream
          .resource(HadoopWriter.jacksonR[F](configuration, schema, path))
          .flatMap(w => (Stream.chunk(leg.head) ++ leg.stream).evalMap(c => w.write(c).as(c.size)))
          .pull
          .echo
      case None => Pull.done
    }.stream
  }

  // parquet
  def parquet(f: Endo[AvroParquetWriter.Builder[GenericRecord]]): Pipe[F, Chunk[GenericRecord], Int] = {
    (ss: Stream[F, Chunk[GenericRecord]]) =>
      ss.pull.stepLeg.flatMap {
        case Some(leg) =>
          val schema = leg.head.flatMap(identity)(0).getSchema
          val writeBuilder = Reader((path: Path) =>
            AvroParquetWriter
              .builder[GenericRecord](HadoopOutputFile.fromPath(path, configuration))
              .withDataModel(GenericData.get())
              .withConf(configuration)
              .withSchema(schema)
              .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
              .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)).map(f)

          Stream
            .resource(HadoopWriter.parquetR[F](writeBuilder, path))
            .flatMap(w => (Stream.chunk(leg.head) ++ leg.stream).evalMap(c => w.write(c).as(c.size)))
            .pull
            .echo
        case None => Pull.done
      }.stream
  }

  val parquet: Pipe[F, Chunk[GenericRecord], Int] =
    parquet(identity)

  // bytes
  val bytes: Pipe[F, Chunk[Byte], Int] = { (ss: Stream[F, Chunk[Byte]]) =>
    Stream
      .resource(HadoopWriter.byteR[F](configuration, path))
      .flatMap(w => ss.evalMap(c => w.write(c).as(c.size)))
  }

  // circe json
  val circe: Pipe[F, Chunk[Json], Int] = { (ss: Stream[F, Chunk[Json]]) =>
    Stream
      .resource(HadoopWriter.stringR[F](configuration, path))
      .flatMap(w => ss.evalMap(c => w.write(c.map(_.noSpaces)).as(c.size)))
  }

  // kantan csv
  def kantan(csvConfiguration: CsvConfiguration): Pipe[F, Chunk[Seq[String]], Int] = {
    (ss: Stream[F, Chunk[Seq[String]]]) =>
      Stream.resource(HadoopWriter.csvStringR[F](configuration, path)).flatMap { w =>
        val process: Stream[F, Int] =
          ss.evalMap(c => w.write(c.map(csvRow(csvConfiguration))).as(c.size))

        val header: Stream[F, Unit] = Stream.eval(w.write(csvHeader(csvConfiguration)))

        if (csvConfiguration.hasHeader) header >> process else process
      }
  }

  def kantan(f: Endo[CsvConfiguration]): Pipe[F, Chunk[Seq[String]], Int] =
    kantan(f(CsvConfiguration.rfc))

  val kantan: Pipe[F, Chunk[Seq[String]], Int] =
    kantan(CsvConfiguration.rfc)

  // text
  val text: Pipe[F, Chunk[String], Int] = { (ss: Stream[F, Chunk[String]]) =>
    Stream
      .resource(HadoopWriter.stringR[F](configuration, path))
      .flatMap(w => ss.evalMap(c => w.write(c).as(c.size)))
  }

  /** Any proto in serialized form must be <2GiB, as that is the maximum size supported by all
    * implementations. It’s recommended to bound request and response sizes.
    *
    * https://protobuf.dev/programming-guides/proto-limits/#total
    */
  val protobuf: Pipe[F, Chunk[GeneratedMessage], Int] = { (ss: Stream[F, Chunk[GeneratedMessage]]) =>
    Stream.resource(HadoopWriter.protobufR(configuration, path)).flatMap { w =>
      ss.evalMap(c => w.write(c).as(c.size))
    }
  }

  // java OutputStream
  val outputStream: Resource[F, OutputStream] =
    HadoopWriter.outputStreamR[F](path, configuration)

}

private object FileSink {
  def apply[F[_]: Sync](configuration: Configuration, path: Url): FileSink[F] =
    new FileSink[F](configuration, toHadoopPath(path))
}
