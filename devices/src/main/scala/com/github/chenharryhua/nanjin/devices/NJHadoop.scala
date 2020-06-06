package com.github.chenharryhua.nanjin.devices

import java.io.InputStream
import java.net.URI

import cats.effect.{Blocker, ContextShift, Resource, Sync}
import fs2.io.readInputStream
import fs2.{Pipe, Stream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}

final class NJHadoop[F[_]: Sync: ContextShift](hadoopConfig: Configuration, blocker: Blocker) {

  private val chunkSize = 8192

  private def fileSystem(pathStr: String): Resource[F, FileSystem] =
    Resource.fromAutoCloseable(blocker.delay(FileSystem.get(new URI(pathStr), hadoopConfig)))

  private def fsOutput(pathStr: String): Resource[F, FSDataOutputStream] =
    for {
      fs <- fileSystem(pathStr)
      rs <- Resource.fromAutoCloseable(blocker.delay(fs.create(new Path(pathStr))))
    } yield rs

  private def fsInput(pathStr: String): Resource[F, FSDataInputStream] =
    for {
      fs <- fileSystem(pathStr)
      rs <- Resource.fromAutoCloseable(blocker.delay(fs.open(new Path(pathStr))))
    } yield rs

  def hadoopSink(pathStr: String): Pipe[F, Byte, Unit] =
    _.chunkLimit(chunkSize).flatMap(bs =>
      Stream.resource(fsOutput(pathStr)).map { os =>
        os.write(bs.toArray)
      })

  def hadoopSource(pathStr: String): Stream[F, Byte] =
    for {
      fs <- Stream.resource(fsInput(pathStr))
      b <- readInputStream(Sync[F].pure[InputStream](fs), chunkSize, blocker)
    } yield b

  def delete(pathStr: String): F[Boolean] =
    fileSystem(pathStr).use(fs => blocker.delay(fs.delete(new Path(pathStr), true)))
}
