package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.implicits.catsSyntaxParallelTraverseNConcurrent
import cats.{Eq, Parallel}
import cats.effect.{Blocker, Concurrent, ContextShift}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat

import scala.reflect.ClassTag
import com.github.chenharryhua.nanjin.common.NJFileFormat.Avro
import com.github.chenharryhua.nanjin.common.NJFileFormat.BinaryAvro
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec

class RddPartitionHoarder[A: ClassTag, K: Eq: ClassTag](
  rdd: RDD[A],
  bucketing: A => Option[K],
  pathBuilder: K => String,
  cfg: HoarderConfig)(implicit codec: NJAvroCodec[A], ss: SparkSession)
    extends Serializable {

  def run[F[_]](
    blocker: Blocker)(implicit F: Concurrent[F], CS: ContextShift[F], P: Parallel[F]): F[Unit] = {
    val params = cfg.evalConfig

    F.bracket(blocker.delay(rdd.persist())) { pr =>
      val keys: List[K] = pr.flatMap(bucketing(_)).distinct().collect().toList
      keys
        .parTraverseN(params.parallelism) { k =>
          val partitionedRDD: RDD[A] = pr.filter(a => bucketing(a).exists(_ === k))
          val path: String           = pathBuilder(k)
          params.format match {
            case NJFileFormat.Avro =>
              new SaveAvro[F, A](partitionedRDD, cfg.withOutPutPath(path)).run(blocker)
            case NJFileFormat.Parquet =>
              new SaveParquet[F, A](partitionedRDD, cfg.withOutPutPath(path)).run(blocker)
            case NJFileFormat.BinaryAvro =>
              new SaveBinaryAvro(partitionedRDD, cfg.withOutPutPath(path)).run(blocker)
            case _ =>
              new SaveJackson(partitionedRDD, cfg.withOutPutPath(path)).run(blocker)
          }
        }
        .void
    }(pr => blocker.delay(pr.unpersist()))
  }
}
