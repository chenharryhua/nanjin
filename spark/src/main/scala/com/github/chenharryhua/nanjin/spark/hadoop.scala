package com.github.chenharryhua.nanjin.spark

import java.net.URI

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import com.sksamuel.avro4s._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

private[spark] object hadoop {

  private def fileSystem[F[_]: Sync: ContextShift](
    pathStr: String,
    config: Configuration,
    blocker: Blocker): Resource[F, FileSystem] =
    Resource.make(blocker.delay(FileSystem.get(new URI(pathStr), config)))(fs =>
      blocker.delay(fs.close()))

  private def outPathResource[F[_]: Sync: ContextShift](
    pathStr: String,
    config: Configuration,
    blocker: Blocker): Resource[F, FSDataOutputStream] =
    fileSystem(pathStr, config, blocker).flatMap(fs =>
      Resource.make(blocker.delay(fs.create(new Path(pathStr))))(a => blocker.delay(a.close())))

  def avroOutputResource[F[_]: Sync: ContextShift, A: SchemaFor](
    pathStr: String,
    config: Configuration,
    builder: AvroOutputStreamBuilder[A],
    blocker: Blocker): Resource[F, AvroOutputStream[A]] =
    hadoop
      .outPathResource(pathStr, config, blocker)
      .flatMap(os =>
        Resource.make(blocker.delay(builder.to(os).build(SchemaFor[A].schema(DefaultFieldMapper))))(
          a => blocker.delay(a.close())))

  private def inPathResource[F[_]: Sync: ContextShift](
    pathStr: String,
    config: Configuration,
    blocker: Blocker): Resource[F, FSDataInputStream] =
    fileSystem(pathStr, config, blocker).flatMap(fs =>
      Resource.make(blocker.delay(fs.open(new Path(pathStr))))(a => blocker.delay(a.close())))

  def avroInputResource[F[_]: Sync: ContextShift, A: SchemaFor](
    pathStr: String,
    config: Configuration,
    builder: AvroInputStreamBuilder[A],
    blocker: Blocker): Resource[F, AvroInputStream[A]] =
    hadoop
      .inPathResource(pathStr, config, blocker)
      .flatMap(is =>
        Resource.make(
          blocker.delay(builder.from(is).build(SchemaFor[A].schema(DefaultFieldMapper))))(a =>
          blocker.delay(a.close())))

}
