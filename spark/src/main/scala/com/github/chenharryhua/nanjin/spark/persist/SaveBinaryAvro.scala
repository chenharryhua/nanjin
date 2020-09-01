package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import com.sksamuel.avro4s.Encoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveBinaryAvro[F[_], A: ClassTag](rdd: RDD[A], cfg: HoarderConfig)(implicit
  codec: NJAvroCodec[A],
  ss: SparkSession)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveBinaryAvro[F, A] =
    new SaveBinaryAvro[F, A](rdd, cfg)

  def overwrite: SaveBinaryAvro[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: SaveBinaryAvro[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: SaveBinaryAvro[F, A] = updateConfig(cfg.withIgnore)

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    implicit val encoder: Encoder[A] = codec.avroEncoder

    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)

    sma.checkAndRun(blocker)(
      rdd.stream[F].through(fileSink[F](blocker).binAvro(params.outPath)).compile.drain)
  }
}
