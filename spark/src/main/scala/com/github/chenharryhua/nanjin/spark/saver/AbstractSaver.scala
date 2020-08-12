package com.github.chenharryhua.nanjin.spark.saver

import cats.Parallel
import cats.effect.implicits._
import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.reflect.ClassTag

abstract private[saver] class AbstractSaver[F[_], A](cfg: SaverConfig) extends Serializable {
  final val params: SaverParams = cfg.evalConfig

  protected def writeSingleFile(rdd: RDD[A], outPath: String, blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit]

  protected def writeMultiFiles(rdd: RDD[A], outPath: String, ss: SparkSession): Unit =
    throw new Exception("not support")

  protected def toDataFrame(rdd: RDD[A])(implicit ss: SparkSession): DataFrame =
    throw new Exception("not support")

  final protected def saveRdd(rdd: RDD[A], outPath: String, blocker: Blocker)(implicit
    F: Concurrent[F],
    ce: ContextShift[F],
    ss: SparkSession): F[Unit] =
    params.singleOrMulti match {
      case SingleOrMulti.Single =>
        params.saveMode match {
          case SaveMode.Append => F.raiseError(new Exception("append mode is not support"))
          case SaveMode.Overwrite =>
            fileSink[F](blocker).delete(outPath) >> writeSingleFile(rdd, outPath, blocker)

          case SaveMode.ErrorIfExists =>
            fileSink[F](blocker).isExist(outPath).flatMap {
              case true  => F.raiseError(new Exception(s"$outPath already exist"))
              case false => writeSingleFile(rdd, outPath, blocker)
            }
          case SaveMode.Ignore =>
            fileSink[F](blocker).isExist(outPath).flatMap {
              case true  => F.pure(())
              case false => writeSingleFile(rdd, outPath, blocker)
            }
        }

      case SingleOrMulti.Multi =>
        params.sparkOrHadoop match {
          case SparkOrHadoop.Spark =>
            F.delay(
              toDataFrame(rdd).write
                .mode(params.saveMode)
                .format(params.fileFormat.format)
                .save(outPath))
          case SparkOrHadoop.Hadoop =>
            params.saveMode match {
              case SaveMode.Append => F.raiseError(new Exception("append mode is not support"))
              case SaveMode.Overwrite =>
                fileSink[F](blocker).delete(outPath) >> F.delay(writeMultiFiles(rdd, outPath, ss))
              case SaveMode.ErrorIfExists =>
                fileSink[F](blocker).isExist(outPath).flatMap {
                  case true  => F.raiseError(new Exception(s"$outPath already exist"))
                  case false => F.delay(writeMultiFiles(rdd, outPath, ss))
                }
              case SaveMode.Ignore =>
                fileSink[F](blocker).isExist(outPath).flatMap {
                  case true  => F.pure(())
                  case false => F.delay(writeMultiFiles(rdd, outPath, ss))
                }
            }
        }
    }

  final protected def savePartitionedRdd[K: ClassTag: Eq](
    rdd: RDD[A],
    blocker: Blocker,
    bucketing: A => K,
    pathBuilder: K => String)(implicit
    F: Concurrent[F],
    CS: ContextShift[F],
    P: Parallel[F],
    ss: SparkSession): F[Unit] =
    F.bracket(blocker.delay(rdd.persist())) { pr =>
      val keys: List[K] = pr.map(bucketing).distinct().collect().toList
      keys
        .parTraverseN(params.parallelism) { k =>
          saveRdd(pr.filter(a => k === bucketing(a)), pathBuilder(k), blocker)
        }
        .void
    }(pr => blocker.delay(pr.unpersist()))
}

private[saver] trait Partition[F[_], A, K] {

  def reBucket[K1: ClassTag: Eq](bucketing: A => K1, pathBuilder: K1 => String): Partition[F, A, K1]

  def rePath(pathBuilder: K => String): Partition[F, A, K]

  def parallel(num: Long): Partition[F, A, K]

  def run(blocker: Blocker)(implicit
    ss: SparkSession,
    F: Concurrent[F],
    CS: ContextShift[F],
    P: Parallel[F]): F[Unit]
}
