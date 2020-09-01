package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import io.circe.{Encoder => JsonEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveCirce[F[_], A: ClassTag](rdd: RDD[A], outPath: String, cfg: HoarderConfig)(implicit
  jsonEncoder: JsonEncoder[A],
  codec: NJAvroCodec[A],
  ss: SparkSession)
    extends Serializable {
  val params: HoarderParams = cfg.evalConfig

  private def updateConfig(cfg: HoarderConfig): SaveCirce[F, A] =
    new SaveCirce[F, A](rdd, outPath, cfg)

  def single: SaveCirce[F, A] = updateConfig(cfg.withSingle)
  def multi: SaveCirce[F, A]  = updateConfig(cfg.withMulti)

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, outPath, ss)
    params.singleOrMulti match {
      case SingleOrMulti.Single =>
        sma.checkAndRun(blocker)(
          rdd
            .map(codec.idConversion)
            .stream[F]
            .through(fileSink[F](blocker).circe(outPath))
            .compile
            .drain)
      case SingleOrMulti.Multi =>
        sma.checkAndRun(blocker)(
          F.delay(
            rdd.map(a => jsonEncoder(codec.idConversion(a)).noSpaces).saveAsTextFile(outPath)))
    }
  }
}
