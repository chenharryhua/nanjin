package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.Sync
import cats.syntax.functor.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.{Pipe, Stream}
import io.scalaland.enumz.Enum
import kantan.csv.*
import kantan.csv.ops.{toCsvInputOps, toCsvOutputOps}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel
import shapeless.{HList, LabelledGeneric}
import shapeless.ops.hlist.ToTraversable
import shapeless.ops.record.Keys

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

final class NJCsv[F[_]] private (
  configuration: Configuration,
  blockSizeHint: Long,
  chunkSize: ChunkSize,
  compressLevel: CompressionLevel,
  csvConfiguration: CsvConfiguration
)(implicit F: Sync[F]) {

  def withChunkSize(cs: ChunkSize): NJCsv[F] =
    new NJCsv[F](configuration, blockSizeHint, cs, compressLevel, csvConfiguration)

  def withBlockSizeHint(bsh: Long): NJCsv[F] =
    new NJCsv[F](configuration, bsh, chunkSize, compressLevel, csvConfiguration)

  def withCompressionLevel(cl: CompressionLevel): NJCsv[F] =
    new NJCsv[F](configuration, blockSizeHint, chunkSize, cl, csvConfiguration)

  def withCompressionLevel(level: Int): NJCsv[F] =
    withCompressionLevel(Enum[CompressionLevel].withIndex(level))

  def source[A](path: NJPath)(implicit dec: HeaderDecoder[A]): Stream[F, A] =
    for {
      is <- Stream.bracket(F.blocking(fileInputStream(path, configuration)))(r => F.blocking(r.close()))
      a <- Stream.fromBlockingIterator(is.asCsvReader[A](csvConfiguration).iterator, chunkSize.value).rethrow
    } yield a

  def sink[A](path: NJPath)(implicit enc: NJHeaderEncoder[A]): Pipe[F, A, Nothing] = { (ss: Stream[F, A]) =>
    Stream
      .bracket(
        F.blocking(fileOutputStream(path, configuration, compressLevel, blockSizeHint).asCsvWriter[A](
          csvConfiguration)))(r => F.blocking(r.close()))
      .flatMap(writer => ss.chunks.foreach(c => F.blocking(c.map(writer.write)).void))
  }

}

object NJCsv {
  def apply[F[_]: Sync](csvCfg: CsvConfiguration, hadoopCfg: Configuration) =
    new NJCsv[F](hadoopCfg, BLOCK_SIZE_HINT, CHUNK_SIZE, CompressionLevel.DEFAULT_COMPRESSION, csvCfg)

  def apply[F[_]: Sync](hadoopCfg: Configuration): NJCsv[F] =
    apply[F](CsvConfiguration.rfc, hadoopCfg)
}
