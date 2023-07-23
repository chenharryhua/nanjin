package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.time.{awakeEvery, Tick}
import fs2.{Pipe, Stream}
import io.scalaland.enumz.Enum
import kantan.csv.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import retry.RetryPolicy
import shapeless.ops.hlist.ToTraversable
import shapeless.ops.record.Keys
import shapeless.{HList, LabelledGeneric}

import scala.annotation.nowarn
sealed trait NJHeaderEncoder[A] extends HeaderEncoder[A]

object NJHeaderEncoder {
  implicit def inferNJHeaderEncoder[A, Repr <: HList, KeysRepr <: HList](implicit
    enc: RowEncoder[A],
    @nowarn gen: LabelledGeneric.Aux[A, Repr],
    keys: Keys.Aux[Repr, KeysRepr],
    traversable: ToTraversable.Aux[KeysRepr, List, Symbol]): NJHeaderEncoder[A] =
    new NJHeaderEncoder[A] {
      override def header: Option[Seq[String]] = Some(keys().toList.map(_.name))
      override def rowEncoder: RowEncoder[A]   = enc
    }
}

final class NJKantan[F[_], A: NJHeaderEncoder: HeaderDecoder] private (
  configuration: Configuration,
  blockSizeHint: Long,
  chunkSize: ChunkSize,
  compressLevel: CompressionLevel,
  csvConfiguration: CsvConfiguration
) {

  def withChunkSize(cs: ChunkSize): NJKantan[F, A] =
    new NJKantan[F, A](configuration, blockSizeHint, cs, compressLevel, csvConfiguration)

  def withBlockSizeHint(bsh: Long): NJKantan[F, A] =
    new NJKantan[F, A](configuration, bsh, chunkSize, compressLevel, csvConfiguration)

  def withCompressionLevel(cl: CompressionLevel): NJKantan[F, A] =
    new NJKantan[F, A](configuration, blockSizeHint, chunkSize, cl, csvConfiguration)

  def withCompressionLevel(level: Int): NJKantan[F, A] =
    withCompressionLevel(Enum[CompressionLevel].withIndex(level))

  def source(path: NJPath)(implicit F: Sync[F]): Stream[F, A] =
    for {
      reader <- Stream.resource(NJReader.kantan(configuration, csvConfiguration, path))
      a <- Stream.fromBlockingIterator(reader.iterator, chunkSize.value).rethrow
    } yield a

  def source(paths: List[NJPath])(implicit F: Sync[F]): Stream[F, A] =
    paths.foldLeft(Stream.empty.covaryAll[F, A]) { case (s, p) =>
      s ++ source(p)
    }

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, A, Nothing] = { (ss: Stream[F, A]) =>
    Stream
      .resource(NJWriter.kantan[F, A](configuration, compressLevel, blockSizeHint, csvConfiguration, path))
      .flatMap(w => persist[F, A](w, ss).stream)
  }

  def sink(policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit F: Async[F]): Pipe[F, A, Nothing] = {
    def getWriter(tick: Tick): Resource[F, NJWriter[F, A]] =
      NJWriter.kantan[F, A](configuration, compressLevel, blockSizeHint, csvConfiguration, pathBuilder(tick))

    val init: Resource[F, (Hotswap[F, NJWriter[F, A]], NJWriter[F, A])] =
      Hotswap(NJWriter
        .kantan[F, A](configuration, compressLevel, blockSizeHint, csvConfiguration, pathBuilder(Tick.Zero)))

    (ss: Stream[F, A]) =>
      Stream.resource(init).flatMap { case (hotswap, writer) =>
        rotatePersist[F, A](
          getWriter,
          hotswap,
          writer,
          ss.map(Left(_)).mergeHaltL(awakeEvery[F](policy).map(Right(_)))
        ).stream
      }
  }

}

object NJKantan {
  def apply[F[_], A: NJHeaderEncoder: HeaderDecoder](
    csvCfg: CsvConfiguration,
    hadoopCfg: Configuration): NJKantan[F, A] =
    new NJKantan[F, A](hadoopCfg, BLOCK_SIZE_HINT, CHUNK_SIZE, CompressionLevel.DEFAULT_COMPRESSION, csvCfg)

  def apply[F[_], A: NJHeaderEncoder: HeaderDecoder](hadoopCfg: Configuration): NJKantan[F, A] =
    apply[F, A](CsvConfiguration.rfc, hadoopCfg)
}
