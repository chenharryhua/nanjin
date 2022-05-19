package com.github.chenharryhua.nanjin.spark.dstream

import cats.data.Reader
import cats.Endo
import com.github.chenharryhua.nanjin.common.NJCompression
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.spark.dstream.DStreamRunner.Mark
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.sksamuel.avro4s.Encoder as AvroEncoder
import io.circe.Encoder as JsonEncoder
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

final class DStreamSink[A](dstream: DStream[A], cfg: SDConfig) extends Serializable {
  val params: SDParams = cfg.evalConfig

  private def updateConfig(f: Endo[SDConfig]): DStreamSink[A] =
    new DStreamSink[A](dstream, f(cfg))

  def pathBuilder(f: NJPath => NJTimestamp => NJPath): DStreamSink[A] = updateConfig(_.pathBuilder(f))

  def transform[B](f: DStream[A] => DStream[B]): DStreamSink[B] = new DStreamSink(f(dstream), cfg)

  def coalesce(implicit tag: ClassTag[A]): DStreamSink[A] = transform(_.transform(_.coalesce(1)))

  def circe(path: NJPath)(implicit enc: JsonEncoder[A]): DStreamCirce[A] =
    new DStreamCirce[A](dstream, Reader(params.pathBuilder(path)), cfg, true)

}

final class AvroDStreamSink[A](dstream: DStream[A], encoder: AvroEncoder[A], cfg: SDConfig) extends Serializable {
  val params: SDParams = cfg.evalConfig

  private def updateConfig(f: Endo[SDConfig]): AvroDStreamSink[A] =
    new AvroDStreamSink[A](dstream, encoder, f(cfg))

  def pathBuilder(f: NJPath => NJTimestamp => NJPath): AvroDStreamSink[A] = updateConfig(_.pathBuilder(f))

  def transform[B](encoder: AvroEncoder[B])(f: DStream[A] => DStream[B]): AvroDStreamSink[B] =
    new AvroDStreamSink(f(dstream), encoder, cfg)

  def coalesce(implicit tag: ClassTag[A]): AvroDStreamSink[A] = transform(encoder)(_.transform(_.coalesce(1)))

  def circe(path: NJPath)(implicit enc: JsonEncoder[A]): DStreamCirce[A] =
    new DStreamCirce[A](dstream, Reader(params.pathBuilder(path)), cfg, true)

  def avro(path: NJPath): DStreamAvro[A] = new DStreamAvro[A](dstream, Reader(params.pathBuilder(path)), encoder, cfg)

  def jackson(path: NJPath): DStreamJackson[A] =
    new DStreamJackson[A](dstream, Reader(params.pathBuilder(path)), encoder, cfg)
}

final class DStreamCirce[A: JsonEncoder](
  dstream: DStream[A],
  pathBuilder: Reader[NJTimestamp, NJPath],
  cfg: SDConfig,
  isKeepNull: Boolean)
    extends Serializable {
  val params: SDParams = cfg.evalConfig

  private def updateConfig(f: Endo[SDConfig]): DStreamCirce[A] =
    new DStreamCirce[A](dstream, pathBuilder, f(cfg), isKeepNull)

  def lz4: DStreamCirce[A]                 = updateConfig(_.withCompression(NJCompression.Lz4))
  def bzip2: DStreamCirce[A]               = updateConfig(_.withCompression(NJCompression.Bzip2))
  def gzip: DStreamCirce[A]                = updateConfig(_.withCompression(NJCompression.Gzip))
  def deflate(level: Int): DStreamCirce[A] = updateConfig(_.withCompression(NJCompression.Deflate(level)))
  def uncompress: DStreamCirce[A]          = updateConfig(_.withCompression(NJCompression.Uncompressed))

  def dropNull: DStreamCirce[A] = new DStreamCirce[A](dstream, pathBuilder, cfg, false)

  def run: Mark = persist.circe(dstream, params.compression, pathBuilder, isKeepNull)
}

final class DStreamAvro[A](
  dstream: DStream[A],
  pathBuilder: Reader[NJTimestamp, NJPath],
  encoder: AvroEncoder[A],
  cfg: SDConfig)
    extends Serializable {
  private def updateConfig(f: Endo[SDConfig]): DStreamAvro[A] =
    new DStreamAvro[A](dstream, pathBuilder, encoder, f(cfg))

  val params: SDParams = cfg.evalConfig

  def deflate(level: Int): DStreamAvro[A] = updateConfig(_.withCompression(NJCompression.Deflate(level)))
  def xz(level: Int): DStreamAvro[A]      = updateConfig(_.withCompression(NJCompression.Xz(level)))
  def snappy: DStreamAvro[A]              = updateConfig(_.withCompression(NJCompression.Snappy))
  def bzip2: DStreamAvro[A]               = updateConfig(_.withCompression(NJCompression.Bzip2))
  def uncompress: DStreamAvro[A]          = updateConfig(_.withCompression(NJCompression.Uncompressed))

  def run: Mark = persist.avro(dstream, encoder, params.compression, pathBuilder)

}

final class DStreamJackson[A](
  dstream: DStream[A],
  pathBuilder: Reader[NJTimestamp, NJPath],
  encoder: AvroEncoder[A],
  cfg: SDConfig)
    extends Serializable {
  private def updateConfig(f: Endo[SDConfig]): DStreamJackson[A] =
    new DStreamJackson[A](dstream, pathBuilder, encoder, f(cfg))

  val params: SDParams = cfg.evalConfig

  def lz4: DStreamJackson[A]                 = updateConfig(_.withCompression(NJCompression.Lz4))
  def bzip2: DStreamJackson[A]               = updateConfig(_.withCompression(NJCompression.Bzip2))
  def gzip: DStreamJackson[A]                = updateConfig(_.withCompression(NJCompression.Gzip))
  def deflate(level: Int): DStreamJackson[A] = updateConfig(_.withCompression(NJCompression.Deflate(level)))
  def uncompress: DStreamJackson[A]          = updateConfig(_.withCompression(NJCompression.Uncompressed))

  def run: Mark = persist.jackson(dstream, encoder, params.compression, pathBuilder)
}
