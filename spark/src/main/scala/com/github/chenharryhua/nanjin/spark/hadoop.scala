package com.github.chenharryhua.nanjin.spark

import java.net.URI

import cats.effect.{Resource, Sync}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import fs2.Stream

private[spark] object hadoop {

  private def fileSystem[F[_]](pathStr: String, config: Configuration)(
    implicit F: Sync[F]): Resource[F, FileSystem] =
    Resource.make(F.delay(FileSystem.get(new URI(pathStr), config)))(fs => F.delay(fs.close()))

  def outResource[F[_]](pathStr: String, config: Configuration)(
    implicit F: Sync[F]): Resource[F, FSDataOutputStream] =
    fileSystem(pathStr, config).flatMap(fs =>
      Resource.make(F.delay(fs.create(new Path(pathStr))))(a => F.delay(a.close())))

  def outStream[F[_]: Sync](pathStr: String, config: Configuration): Stream[F, FSDataOutputStream] =
    Stream.resource(outResource(pathStr, config))

  def inResource[F[_]](pathStr: String, config: Configuration)(
    implicit F: Sync[F]): Resource[F, FSDataInputStream] =
    fileSystem(pathStr, config).flatMap(fs =>
      Resource.make(F.delay(fs.open(new Path(pathStr))))(a => F.delay(a.close())))

  def inStream[F[_]: Sync](pathStr: String, config: Configuration): Stream[F, FSDataInputStream] =
    Stream.resource(inResource(pathStr, config))
}
