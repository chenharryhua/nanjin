package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.datetime.tickStream
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import fs2.{Chunk, Pipe, Stream}
import kantan.csv.*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
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

final class HadoopKantan[F[_]] private (
  configuration: Configuration,
  blockSizeHint: Long,
  chunkSize: ChunkSize,
  compressLevel: CompressionLevel,
  csvConfiguration: CsvConfiguration
) {

  // config

  def withChunkSize(cs: ChunkSize): HadoopKantan[F] =
    new HadoopKantan[F](configuration, blockSizeHint, cs, compressLevel, csvConfiguration)

  def withBlockSizeHint(bsh: Long): HadoopKantan[F] =
    new HadoopKantan[F](configuration, bsh, chunkSize, compressLevel, csvConfiguration)

  def withCompressionLevel(cl: CompressionLevel): HadoopKantan[F] =
    new HadoopKantan[F](configuration, blockSizeHint, chunkSize, cl, csvConfiguration)

  // read

  def source[A: HeaderDecoder](path: NJPath)(implicit F: Sync[F]): Stream[F, A] =
    for {
      reader <- HadoopReader.kantanS(configuration, csvConfiguration, path.hadoopPath)
      a <- Stream.fromBlockingIterator(reader.iterator, chunkSize.value).rethrow
    } yield a

  def source[A: HeaderDecoder](paths: List[NJPath])(implicit F: Sync[F]): Stream[F, A] =
    paths.foldLeft(Stream.empty.covaryAll[F, A]) { case (s, p) =>
      s ++ source(p)
    }

  // write

  private def getWriterR[A: NJHeaderEncoder](path: Path)(implicit
    F: Sync[F]): Resource[F, HadoopWriter[F, A]] =
    HadoopWriter.kantanR[F, A](configuration, compressLevel, blockSizeHint, csvConfiguration, path)

  def sink[A: NJHeaderEncoder](path: NJPath)(implicit F: Sync[F]): Pipe[F, Chunk[A], Nothing] = {
    (ss: Stream[F, Chunk[A]]) =>
      Stream.resource(getWriterR(path.hadoopPath)).flatMap(w => ss.foreach(w.write))
  }

  def sink[A: NJHeaderEncoder](policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Chunk[A], Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, A]] =
      getWriterR(pathBuilder(tick).hadoopPath)

    def init(tick: Tick): Resource[F, (Hotswap[F, HadoopWriter[F, A]], HadoopWriter[F, A])] =
      Hotswap(getWriter(tick))

    // save
    (ss: Stream[F, Chunk[A]]) =>
      Stream.eval(Tick.Zero).flatMap { zero =>
        Stream.resource(init(zero)).flatMap { case (hotswap, writer) =>
          persist[F, A](
            getWriter,
            hotswap,
            writer,
            ss.map(Left(_)).mergeHaltBoth(tickStream[F](policy, zero).map(Right(_)))
          ).stream
        }
      }
  }
}

object HadoopKantan {
  def apply[F[_]](hadoopCfg: Configuration, csvCfg: CsvConfiguration): HadoopKantan[F] =
    new HadoopKantan[F](hadoopCfg, BLOCK_SIZE_HINT, CHUNK_SIZE, CompressionLevel.DEFAULT_COMPRESSION, csvCfg)
}
