package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.Hotswap
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.datetime.tickStream
import com.github.chenharryhua.nanjin.datetime.tickStream.Tick
import fs2.text.utf8
import fs2.{Chunk, Pipe, Stream}
import kantan.csv.*
import kantan.csv.CsvConfiguration.Header
import kantan.csv.engine.{ReaderEngine, WriterEngine}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import retry.RetryPolicy

import java.io.{InputStreamReader, StringWriter}
import java.nio.charset.StandardCharsets

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

  private def buildCsvRow(row: Seq[String])(implicit engine: WriterEngine): String = {
    val sw = new StringWriter()
    engine.writerFor(sw, csvConfiguration).write(row).close()
    sw.toString.dropRight(2) // drop CRLF
  }

  private lazy val header: Chunk[String] = csvConfiguration.header match {
    case Header.None             => Chunk.empty
    case Header.Implicit         => Chunk("no header explicitly provided")
    case Header.Explicit(header) => Chunk(buildCsvRow(header))
  }

  def sink(path: NJPath)(implicit F: Sync[F]): Pipe[F, Chunk[Seq[String]], Nothing] = {
    (ss: Stream[F, Chunk[Seq[String]]]) =>
      Stream
        .resource(HadoopWriter.byteR[F](configuration, compressLevel, blockSizeHint, path.hadoopPath))
        .flatMap { w =>
          val src: Stream[F, Chunk[String]] = Stream(header) ++ ss.map(_.map(buildCsvRow))
          src.unchunks.intersperse(NEWLINE_SEPARATOR).through(utf8.encode).chunks.foreach(w.write)
        }
  }

  def sink(policy: RetryPolicy[F])(pathBuilder: Tick => NJPath)(implicit
    F: Async[F]): Pipe[F, Chunk[Seq[String]], Nothing] = {
    def getWriter(tick: Tick): Resource[F, HadoopWriter[F, String]] =
      HadoopWriter.stringR[F](
        configuration,
        compressLevel,
        blockSizeHint,
        StandardCharsets.UTF_8,
        pathBuilder(tick).hadoopPath)

    def init(tick: Tick): Resource[F, (Hotswap[F, HadoopWriter[F, String]], HadoopWriter[F, String])] =
      Hotswap(getWriter(tick))

    // save
    (ss: Stream[F, Chunk[Seq[String]]]) =>
      Stream.eval(Tick.Zero).flatMap { zero =>
        Stream.resource(init(zero)).flatMap { case (hotswap, writer) =>
          val src: Stream[F, Chunk[String]] = Stream(header) ++ ss.map(_.map(buildCsvRow))
          persistString[F](
            getWriter,
            hotswap,
            writer,
            src.map(Left(_)).mergeHaltBoth(tickStream[F](policy, zero).map(Right(_))),
            Chunk.empty,
            header.map(_.concat(NEWLINE_SEPARATOR))
          ).stream
        }
      }
  }
}

object HadoopKantan {
  def apply[F[_]](hadoopCfg: Configuration, csvCfg: CsvConfiguration): HadoopKantan[F] =
    new HadoopKantan[F](hadoopCfg, BLOCK_SIZE_HINT, CHUNK_SIZE, CompressionLevel.DEFAULT_COMPRESSION, csvCfg)
}
