package com.github.chenharryhua.nanjin.pipes

import java.io.OutputStream
import java.net.URI

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import com.sksamuel.avro4s._
import kantan.csv.{CsvConfiguration, CsvWriter, HeaderEncoder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import cats.implicits._

object hadoop {

  private def fileSystem[F[_]: Sync: ContextShift](
    pathStr: String,
    hadoopConfig: Configuration,
    blocker: Blocker): Resource[F, FileSystem] =
    Resource.make(Sync[F].delay(FileSystem.get(new URI(pathStr), hadoopConfig)))(fs =>
      blocker.delay(fs.close()))

  def outputPathResource[F[_]: Sync: ContextShift](
    pathStr: String,
    hadoopConfig: Configuration,
    blocker: Blocker): Resource[F, FSDataOutputStream] =
    fileSystem(pathStr, hadoopConfig, blocker).flatMap(fs =>
      Resource.make(Sync[F].delay(fs.create(new Path(pathStr))))(a => blocker.delay(a.close())))

  def avroOutputResource[F[_]: Sync: ContextShift, A: SchemaFor](
    pathStr: String,
    hadoopConfig: Configuration,
    builder: AvroOutputStreamBuilder[A],
    blocker: Blocker): Resource[F, AvroOutputStream[A]] =
    outputPathResource(pathStr, hadoopConfig, blocker).flatMap(os =>
      Resource.make(Sync[F].delay(builder.to(os).build(SchemaFor[A].schema(DefaultFieldMapper))))(
        a => blocker.delay(a.close())))

  def csvOutputResource[F[_]: Sync: ContextShift, A: HeaderEncoder](
    pathStr: String,
    hadoopConfig: Configuration,
    blocker: Blocker,
    csvConfig: CsvConfiguration): Resource[F, CsvWriter[A]] = {
    import kantan.csv.ops._
    outputPathResource(pathStr, hadoopConfig, blocker)
      .widen[OutputStream]
      .flatMap(os =>
        Resource.make(Sync[F].delay(os.asCsvWriter[A](csvConfig)))(a => blocker.delay(a.close())))
  }

  def inputPathResource[F[_]: Sync: ContextShift](
    pathStr: String,
    hadoopConfig: Configuration,
    blocker: Blocker): Resource[F, FSDataInputStream] =
    fileSystem(pathStr, hadoopConfig, blocker).flatMap(fs =>
      Resource.make(Sync[F].delay(fs.open(new Path(pathStr))))(a => blocker.delay(a.close())))

  def avroInputResource[F[_]: Sync: ContextShift, A: SchemaFor](
    pathStr: String,
    hadoopConfig: Configuration,
    builder: AvroInputStreamBuilder[A],
    blocker: Blocker): Resource[F, AvroInputStream[A]] =
    inputPathResource(pathStr, hadoopConfig, blocker).flatMap(is =>
      Resource.make(Sync[F].delay(builder.from(is).build(SchemaFor[A].schema(DefaultFieldMapper))))(
        a => blocker.delay(a.close())))

  def delete[F[_]: Sync: ContextShift](
    pathStr: String,
    hadoopConfig: Configuration,
    blocker: Blocker): F[Boolean] =
    fileSystem(pathStr, hadoopConfig, blocker).use(fs =>
      blocker.delay(fs.delete(new Path(pathStr), true)))

}
