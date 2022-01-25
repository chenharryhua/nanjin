package mtest.terminals

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

class ParquetTest extends AnyFunSuite {
  import HadoopTestData.*
  val pq = new NJParquet[IO]()

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
      ts.through(pq.parquetSink(wb)).compile.drain >>
      pq.parquetSource(rb).compile.toList

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
      ts.through(pq.parquetSink(wb)).compile.drain >>
      pq.parquetSource(rb).compile.toList

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
      hdp.delete(pathStr) >> ts.through(pq.parquetSink(wb)).compile.drain >> pq.parquetSource(rb).compile.toList

    assert(action.unsafeRunSync() == pandas)
  }
}
