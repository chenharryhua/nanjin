package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag

final class SaveText[F[_], A: ClassTag](
  rdd: RDD[A],
  outPath: String,
  sma: SaveModeAware[F],
  cfg: SaverConfig)(implicit show: Show[A], codec: NJAvroCodec[A], ss: SparkSession)
    extends Serializable {
  val params: SaverParams = cfg.evalConfig

  def run(blocker: Blocker)(implicit F: Concurrent[F], cs: ContextShift[F]): F[Unit] =
    params.singleOrMulti match {
      case SingleOrMulti.Single =>
        sma.run(
          rdd.stream[F].through(fileSink[F](blocker).text(outPath)).compile.drain,
          outPath,
          blocker)
      case SingleOrMulti.Multi =>
        sma.run(
          F.delay(rdd.map(a => show.show(codec.idConversion(a))).saveAsTextFile(outPath)),
          outPath,
          blocker)
    }
}
