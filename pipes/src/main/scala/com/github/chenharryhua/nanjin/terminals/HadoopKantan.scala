package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.datetime.tickStream
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import fs2.{Chunk, Pipe, Stream}
import kantan.csv.*
import kantan.csv.engine.ReaderEngine
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import retry.RetryPolicy

import java.io.InputStreamReader

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

  def source(path: NJPath)(implicit F: Sync[F], engine: ReaderEngine): Stream[F, Seq[String]] =
    HadoopReader.inputStreamS[F](configuration, path.hadoopPath).flatMap { is =>
      val reader: CsvReader[Seq[String]] =
        if (csvConfiguration.hasHeader)
          engine.unsafeReaderFor(new InputStreamReader(is), csvConfiguration).drop(1)
        else
          engine.unsafeReaderFor(new InputStreamReader(is), csvConfiguration)

      Stream.fromBlockingIterator[F](reader.iterator, chunkSize.value)
    }

  def source(paths: List[NJPath])(implicit F: Sync[F]): Stream[F, Seq[String]] =
    paths.foldLeft(Stream.empty.covaryAll[F, Seq[String]]) { case (s, p) => s ++ source(p) }

  // write

  private def getWriterR(path: Path)(implicit F: Sync[F]): Resource[F, HadoopWriter[F, Seq[String]]] =
    HadoopWriter.kantanR[F](configuration, compressLevel, blockSizeHint, csvConfiguration, path)

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Chunk[Seq[String]], Nothing] = {
    (ss: Stream[F, Chunk[Seq[String]]]) =>
      Stream.resource(getWriterR(path.hadoopPath)).flatMap(w => ss.foreach(w.write))
  }

  def sink(policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Chunk[Seq[String]], Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, Seq[String]]] =
      getWriterR(pathBuilder(tick).hadoopPath)

    def init(
      tick: Tick): Resource[F, (Hotswap[F, HadoopWriter[F, Seq[String]]], HadoopWriter[F, Seq[String]])] =
      Hotswap(getWriter(tick))

    // save
    (ss: Stream[F, Chunk[Seq[String]]]) =>
      Stream.eval(Tick.Zero).flatMap { zero =>
        Stream.resource(init(zero)).flatMap { case (hotswap, writer) =>
          persist[F, Seq[String]](
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
