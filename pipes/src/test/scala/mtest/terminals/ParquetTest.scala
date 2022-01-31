package mtest.terminals

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.{NJParquet, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.{HadoopInputFile, HadoopOutputFile}
import org.scalatest.funsuite.AnyFunSuite
import cats.Eval

class ParquetTest extends AnyFunSuite {
  import HadoopTestData.*

  test("snappy parquet write/read") {
    import HadoopTestData.*
    val pathStr = NJPath("./data/test/devices/builder/panda.snappy.parquet")
    val ts      = Stream.emits(pandas).covary[IO]
    val wb = AvroParquetWriter
      .builder[GenericRecord](HadoopOutputFile.fromPath(pathStr.hadoopPath, cfg))
      .withSchema(pandaSchema)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .withDataModel(GenericData.get())
      .withConf(cfg)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)

    val rb = IO(
      AvroParquetReader
        .builder[GenericRecord](HadoopInputFile.fromPath(pathStr.hadoopPath, cfg))
        .withDataModel(GenericData.get()))

    val action = hdp.delete(pathStr) >>
      ts.through(NJParquet.fs2Sink(wb)).compile.drain >>
      NJParquet.fs2Source(rb).compile.toList

    assert(action.unsafeRunSync() == pandas)
  }

  test("gzip parquet write/read") {
    val pathStr = NJPath("./data/test/devices/panda.gzip.parquet")
    val ts      = Stream.emits(pandas).covary[IO]
    val wb = AvroParquetWriter
      .builder[GenericRecord](HadoopOutputFile.fromPath(pathStr.hadoopPath, cfg))
      .withSchema(pandaSchema)
      .withCompressionCodec(CompressionCodecName.GZIP)
      .withDataModel(GenericData.get())
      .withConf(cfg)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)

    val rb = IO(
      AvroParquetReader
        .builder[GenericRecord](HadoopInputFile.fromPath(pathStr.hadoopPath, cfg))
        .withDataModel(GenericData.get()))

    val action = hdp.delete(pathStr) >>
      ts.through(NJParquet.fs2Sink(wb)).compile.drain >>
      NJParquet.fs2Source(rb).compile.toList

    assert(action.unsafeRunSync() == pandas)
  }
  test("uncompressed parquet write/read") {
    val pathStr = NJPath("./data/test/devices/panda.uncompressed.parquet")
    val ts      = Stream.emits(pandas).covary[IO]
    val wb = AvroParquetWriter
      .builder[GenericRecord](HadoopOutputFile.fromPath(pathStr.hadoopPath, cfg))
      .withSchema(pandaSchema)
      .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
      .withDataModel(GenericData.get())
      .withConf(cfg)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)

    val rb = IO(
      AvroParquetReader
        .builder[GenericRecord](HadoopInputFile.fromPath(pathStr.hadoopPath, cfg))
        .withDataModel(GenericData.get()))

    val action =
      hdp.delete(pathStr) >> ts.through(NJParquet.fs2Sink(wb)).compile.drain >> NJParquet.fs2Source(rb).compile.toList

    assert(action.unsafeRunSync() == pandas)
  }

  implicit val mat: Materializer = Materializer(akkaSystem)
  test("uncompressed parquet write/read akka") {
    val pathStr = NJPath("./data/test/devices/akka/panda.uncompressed.parquet")
    val ts      = Source(pandas)
    val wb = AvroParquetWriter
      .builder[GenericRecord](HadoopOutputFile.fromPath(pathStr.hadoopPath, cfg))
      .withSchema(pandaSchema)
      .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
      .withDataModel(GenericData.get())
      .withConf(cfg)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)

    val rb = Eval.later(
      AvroParquetReader
        .builder[GenericRecord](HadoopInputFile.fromPath(pathStr.hadoopPath, cfg))
        .withDataModel(GenericData.get()))

    val write = IO.fromFuture(IO(ts.runWith(NJParquet.akkaSink(wb))))
    val read  = IO.fromFuture(IO(NJParquet.akkaSource(rb).runFold(List.empty[GenericRecord])(_.appended(_))))
    val rst   = (write >> read).unsafeRunSync()
    assert(rst == pandas)
  }
}
