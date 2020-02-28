package com.github.chenharryhua.nanjin.pipes

import java.io.OutputStream
import java.net.URI

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import cats.implicits._
import com.sksamuel.avro4s._
import kantan.csv.{CsvConfiguration, CsvWriter, HeaderEncoder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

object hadoop {

  def fileSystem[F[_]: Sync: ContextShift](
    pathStr: String,
    hadoopConfig: Configuration,
    blocker: Blocker): Resource[F, FileSystem] =
    Resource.make(Sync[F].delay(FileSystem.get(new URI(pathStr), hadoopConfig)))(fs =>
      blocker.delay(fs.close()))

  def outputPathResource[F[_]: Sync: ContextShift](
    pathStr: String,
    hadoopConfig: Configuration,
    blocker: Blocker): Resource[F, FSDataOutputStream] =
    for {
      fs <- fileSystem(pathStr, hadoopConfig, blocker)
      rs <- Resource.make(Sync[F].delay(fs.create(new Path(pathStr))))(a =>
        blocker.delay(a.close()))
    } yield rs

  def avroOutputResource[F[_]: Sync: ContextShift, A: SchemaFor](
    pathStr: String,
    hadoopConfig: Configuration,
    builder: AvroOutputStreamBuilder[A],
    blocker: Blocker): Resource[F, AvroOutputStream[A]] =
    for {
      os <- outputPathResource(pathStr, hadoopConfig, blocker)
      rs <- Resource.make(
        Sync[F].delay(builder.to(os).build(SchemaFor[A].schema(DefaultFieldMapper))))(a =>
        blocker.delay(a.close()))
    } yield rs

  def csvOutputResource[F[_]: Sync: ContextShift, A: HeaderEncoder](
    pathStr: String,
    hadoopConfig: Configuration,
    blocker: Blocker,
    csvConfig: CsvConfiguration): Resource[F, CsvWriter[A]] = {
    import kantan.csv.ops._
    for {
      os <- outputPathResource(pathStr, hadoopConfig, blocker).widen[OutputStream]
      rs <- Resource.make(Sync[F].delay(os.asCsvWriter[A](csvConfig)))(a =>
        blocker.delay(a.close()))
    } yield rs
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
    for {
      is <- inputPathResource(pathStr, hadoopConfig, blocker)
      rs <- Resource.make(
        Sync[F].delay(builder.from(is).build(SchemaFor[A].schema(DefaultFieldMapper))))(a =>
        blocker.delay(a.close()))
    } yield rs

  def delete[F[_]: Sync: ContextShift](
    pathStr: String,
    hadoopConfig: Configuration,
    blocker: Blocker): F[Boolean] =
    fileSystem(pathStr, hadoopConfig, blocker).use(fs =>
      blocker.delay(fs.delete(new Path(pathStr), true)))

}
