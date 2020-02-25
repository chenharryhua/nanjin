package com.github.chenharryhua.nanjin.spark

import java.io.OutputStream
import java.net.URI

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import com.sksamuel.avro4s._
import kantan.csv.{CsvWriter, HeaderEncoder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

private[spark] object hadoop {

  private def fileSystem[F[_]: Sync: ContextShift](
    pathStr: String,
    config: Configuration,
    blocker: Blocker): Resource[F, FileSystem] =
    Resource.make(Sync[F].delay(FileSystem.get(new URI(pathStr), config)))(fs =>
      blocker.delay(fs.close()))

  def outputPathResource[F[_]: Sync: ContextShift](
    pathStr: String,
    config: Configuration,
    blocker: Blocker): Resource[F, FSDataOutputStream] =
    fileSystem(pathStr, config, blocker).flatMap(fs =>
      Resource.make(Sync[F].delay(fs.create(new Path(pathStr))))(a => blocker.delay(a.close())))

  def avroOutputResource[F[_]: Sync: ContextShift, A: SchemaFor](
    pathStr: String,
    config: Configuration,
    builder: AvroOutputStreamBuilder[A],
    blocker: Blocker): Resource[F, AvroOutputStream[A]] =
    outputPathResource(pathStr, config, blocker).flatMap(os =>
      Resource.make(Sync[F].delay(builder.to(os).build(SchemaFor[A].schema(DefaultFieldMapper))))(
        a => blocker.delay(a.close())))

  def csvOutputResource[F[_]: Sync: ContextShift, A: HeaderEncoder](
    pathStr: String,
    config: Configuration,
    blocker: Blocker): Resource[F, CsvWriter[A]] = {
    import kantan.csv.ops._
    import kantan.csv.rfc
    outputPathResource(pathStr, config, blocker)
      .widen[OutputStream]
      .flatMap(os =>
        Resource.make(Sync[F].delay(os.asCsvWriter[A](rfc.withHeader)))(a =>
          blocker.delay(a.close())))
  }

  def inputPathResource[F[_]: Sync: ContextShift](
    pathStr: String,
    config: Configuration,
    blocker: Blocker): Resource[F, FSDataInputStream] =
    fileSystem(pathStr, config, blocker).flatMap(fs =>
      Resource.make(Sync[F].delay(fs.open(new Path(pathStr))))(a => blocker.delay(a.close())))

  def avroInputResource[F[_]: Sync: ContextShift, A: SchemaFor](
    pathStr: String,
    config: Configuration,
    builder: AvroInputStreamBuilder[A],
    blocker: Blocker): Resource[F, AvroInputStream[A]] =
    inputPathResource(pathStr, config, blocker).flatMap(is =>
      Resource.make(Sync[F].delay(builder.from(is).build(SchemaFor[A].schema(DefaultFieldMapper))))(
        a => blocker.delay(a.close())))

  def delete[F[_]: Sync: ContextShift](
    pathStr: String,
    config: Configuration,
    blocker: Blocker): F[Boolean] =
    fileSystem(pathStr, config, blocker).use(fs =>
      blocker.delay(fs.delete(new Path(pathStr), true)))

}
