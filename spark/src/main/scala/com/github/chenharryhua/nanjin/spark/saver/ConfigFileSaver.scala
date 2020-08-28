package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark.fileSink
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.ClassTag

final class ConfigFileSaver[F[_], A](
  rdd: RDD[A],
  outPath: String,
  writer: NJWriter[F, A],
  cfg: SaverConfig) {

  val params: SaverParams = cfg.evalConfig

  def withPath(pathStr: String): ConfigFileSaver[F, A] =
    new ConfigFileSaver[F, A](rdd, pathStr, writer, cfg)

  def withRDD(rdd: RDD[A]): ConfigFileSaver[F, A] =
    new ConfigFileSaver[F, A](rdd, outPath, writer, cfg)

  private def updateConfig(cfg: SaverConfig): ConfigFileSaver[F, A] =
    new ConfigFileSaver[F, A](rdd, outPath, writer, cfg)

  def repartition(num: Int): ConfigFileSaver[F, A] =
    new ConfigFileSaver[F, A](rdd.repartition(num), outPath, writer, cfg)

  def overwrite: ConfigFileSaver[F, A]      = updateConfig(cfg.withOverwrite)
  def errorIfExists: ConfigFileSaver[F, A]  = updateConfig(cfg.withError)
  def ignoreIfExists: ConfigFileSaver[F, A] = updateConfig(cfg.withIgnore)

  def single: ConfigFileSaver[F, A] = updateConfig(cfg.withSingle)
  def multi: ConfigFileSaver[F, A]  = updateConfig(cfg.withMulti)

  def partition[K: ClassTag: Eq](bucketing: A => Option[K])(
    pathBuilder: K => String): ConfigPartitionSaver[F, A, K] =
    new ConfigPartitionSaver[F, A, K](rdd, this, params.parallelism, bucketing, pathBuilder)

  def run(
    blocker: Blocker)(implicit F: Concurrent[F], ce: ContextShift[F], ss: SparkSession): F[Unit] =
    params.singleOrMulti match {
      case SingleOrMulti.Single =>
        params.saveMode match {
          case SaveMode.Append => F.raiseError(new Exception("append mode is not support"))
          case SaveMode.Overwrite =>
            fileSink[F](blocker)
              .delete(outPath) >> writer.writeSingleFile(rdd, outPath, ss, blocker)

          case SaveMode.ErrorIfExists =>
            fileSink[F](blocker).isExist(outPath).flatMap {
              case true  => F.raiseError(new Exception(s"$outPath already exist"))
              case false => writer.writeSingleFile(rdd, outPath, ss, blocker)
            }
          case SaveMode.Ignore =>
            fileSink[F](blocker).isExist(outPath).flatMap {
              case true  => F.pure(())
              case false => writer.writeSingleFile(rdd, outPath, ss, blocker)
            }
        }

      case SingleOrMulti.Multi =>
        params.saveMode match {
          case SaveMode.Append => F.raiseError(new Exception("append mode is not support"))
          case SaveMode.Overwrite =>
            fileSink[F](blocker).delete(outPath) >> F.delay(
              writer.writeMultiFiles(rdd, outPath, ss))
          case SaveMode.ErrorIfExists =>
            fileSink[F](blocker).isExist(outPath).flatMap {
              case true  => F.raiseError(new Exception(s"$outPath already exist"))
              case false => F.delay(writer.writeMultiFiles(rdd, outPath, ss))
            }
          case SaveMode.Ignore =>
            fileSink[F](blocker).isExist(outPath).flatMap {
              case true  => F.pure(())
              case false => F.delay(writer.writeMultiFiles(rdd, outPath, ss))
            }
        }
    }
}
