package com.github.chenharryhua.nanjin.pipes

import java.io.OutputStream
import java.net.URI

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import com.sksamuel.avro4s.{
  AvroInputStream,
  AvroInputStreamBuilder,
  AvroOutputStream,
  AvroOutputStreamBuilder
}
import kantan.csv.{CsvConfiguration, CsvWriter, HeaderEncoder}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}

object hadoop {

  def fileSystem[F[_]: Sync: ContextShift](
    pathStr: String,
    hadoopConfig: Configuration,
    blocker: Blocker): Resource[F, FileSystem] =
    Resource.fromAutoCloseable(blocker.delay(FileSystem.get(new URI(pathStr), hadoopConfig)))

  def outputPathResource[F[_]: Sync: ContextShift](
    pathStr: String,
    hadoopConfig: Configuration,
    blocker: Blocker): Resource[F, FSDataOutputStream] =
    for {
      fs <- fileSystem(pathStr, hadoopConfig, blocker)
      rs <- Resource.fromAutoCloseable(blocker.delay(fs.create(new Path(pathStr))))
    } yield rs

  def avroOutputResource[F[_]: Sync: ContextShift, A](
    pathStr: String,
    schema: Schema,
    hadoopConfig: Configuration,
    builder: AvroOutputStreamBuilder[A],
    blocker: Blocker): Resource[F, AvroOutputStream[A]] =
    for {
      os <- outputPathResource(pathStr, hadoopConfig, blocker)
      rs <- Resource.fromAutoCloseable(Sync[F].pure(builder.to(os).build(schema)))
    } yield rs

  def parquetOutputResource[F[_]: Sync: ContextShift](
    pathStr: String,
    schema: Schema,
    hadoopConfig: Configuration,
    blocker: Blocker): Resource[F, ParquetWriter[GenericRecord]] =
    Resource.fromAutoCloseable(
      blocker.delay(
        AvroParquetWriter
          .builder[GenericRecord](new Path(pathStr))
          .withConf(hadoopConfig)
          .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .withSchema(schema)
          .withDataModel(GenericData.get())
          .build()))

  def csvOutputResource[F[_]: Sync: ContextShift, A: HeaderEncoder](
    pathStr: String,
    hadoopConfig: Configuration,
    blocker: Blocker,
    csvConfig: CsvConfiguration): Resource[F, CsvWriter[A]] = {
    import kantan.csv.ops._
    for {
      os <- outputPathResource(pathStr, hadoopConfig, blocker).widen[OutputStream]
      rs <- Resource.fromAutoCloseable(Sync[F].pure(os.asCsvWriter[A](csvConfig)))
    } yield rs
  }

  def inputPathResource[F[_]: Sync: ContextShift](
    pathStr: String,
    hadoopConfig: Configuration,
    blocker: Blocker): Resource[F, FSDataInputStream] =
    for {
      fs <- fileSystem(pathStr, hadoopConfig, blocker)
      rs <- Resource.fromAutoCloseable(blocker.delay(fs.open(new Path(pathStr))))
    } yield rs

  def avroInputResource[F[_]: Sync: ContextShift, A](
    pathStr: String,
    schema: Schema,
    hadoopConfig: Configuration,
    builder: AvroInputStreamBuilder[A],
    blocker: Blocker): Resource[F, AvroInputStream[A]] =
    for {
      is <- inputPathResource(pathStr, hadoopConfig, blocker)
      rs <- Resource.fromAutoCloseable(Sync[F].pure(builder.from(is).build(schema)))
    } yield rs

  def delete[F[_]: Sync: ContextShift](
    pathStr: String,
    hadoopConfig: Configuration,
    blocker: Blocker): F[Boolean] =
    fileSystem(pathStr, hadoopConfig, blocker).use(fs =>
      blocker.delay(fs.delete(new Path(pathStr), true)))

}
