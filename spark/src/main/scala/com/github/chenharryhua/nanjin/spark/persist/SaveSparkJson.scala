package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class SaveSparkJson[F[_], A: ClassTag](rdd: RDD[A], cfg: HoarderConfig)(implicit
  codec: NJAvroCodec[A],
  ss: SparkSession)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] = {
    val sma: SaveModeAware[F] = new SaveModeAware[F](params.saveMode, params.outPath, ss)

    sma.checkAndRun(blocker)(
      F.delay(
        utils
          .normalizedDF(rdd, codec.avroEncoder)
          .write
          .mode(SaveMode.Overwrite)
          .json(params.outPath)))
  }
}
