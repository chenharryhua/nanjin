package com.github.chenharryhua.nanjin.spark

import java.net.URI

import cats.effect.{Resource, Sync}
import com.sksamuel.avro4s._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

private[spark] object hadoop {

  private def fileSystem[F[_]](pathStr: String, config: Configuration)(
    implicit F: Sync[F]): Resource[F, FileSystem] =
    Resource.make(F.delay(FileSystem.get(new URI(pathStr), config)))(fs => F.delay(fs.close()))

  private def outPathResource[F[_]](pathStr: String, config: Configuration)(
    implicit F: Sync[F]): Resource[F, FSDataOutputStream] =
    fileSystem(pathStr, config).flatMap(fs =>
      Resource.make(F.delay(fs.create(new Path(pathStr))))(a => F.delay(a.close())))

  def avroOutputResource[F[_], A: SchemaFor](
    pathStr: String,
    config: Configuration,
    builder: AvroOutputStreamBuilder[A])(implicit F: Sync[F]): Resource[F, AvroOutputStream[A]] =
    hadoop
      .outPathResource(pathStr, config)
      .flatMap(os =>
        Resource.make(F.delay(builder.to(os).build(SchemaFor[A].schema(DefaultFieldMapper))))(a =>
          F.delay(a.close())))

  private def inPathResource[F[_]](pathStr: String, config: Configuration)(
    implicit F: Sync[F]): Resource[F, FSDataInputStream] =
    fileSystem(pathStr, config).flatMap(fs =>
      Resource.make(F.delay(fs.open(new Path(pathStr))))(a => F.delay(a.close())))

  def avroInputResource[F[_], A: SchemaFor](
    pathStr: String,
    config: Configuration,
    builder: AvroInputStreamBuilder[A])(implicit F: Sync[F]): Resource[F, AvroInputStream[A]] =
    hadoop
      .inPathResource(pathStr, config)
      .flatMap(is =>
        Resource.make(F.delay(builder.from(is).build(SchemaFor[A].schema(DefaultFieldMapper))))(a =>
          F.delay(a.close())))

}
