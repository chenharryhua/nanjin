package com.github.chenharryhua.nanjin.spark.dstream

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import io.circe.{Encoder => JsonEncoder}
import monocle.Getter
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

final class SparkDStream[A](val dstream: DStream[A], cfg: SDConfig) extends Serializable {
  val params: SDParams = cfg.evalConfig

  private def updateConfig(f: SDConfig => SDConfig): SparkDStream[A] =
    new SparkDStream[A](dstream, f(cfg))

  def checkpoint(fd: FiniteDuration): SparkDStream[A] = updateConfig(_.withCheckpointDuration(fd))

  def transform[B: ClassTag](f: RDD[A] => RDD[B]): SparkDStream[B] = new SparkDStream[B](dstream.transform(f), cfg)

  def circe(path: String)(implicit enc: JsonEncoder[A]): EndMark = persist.circe[A](dstream)(params.pathBuilder(path))
}

object SparkDStream {

  implicit def getterSparkDStream[A]: Getter[SparkDStream[A], DStream[A]] =
    (s: SparkDStream[A]) => s.dstream
}

final class SparkAvroDStream[A](val dstream: DStream[A], encoder: AvroEncoder[A], cfg: SDConfig) extends Serializable {
  val params: SDParams = cfg.evalConfig

  private def updateConfig(f: SDConfig => SDConfig): SparkAvroDStream[A] =
    new SparkAvroDStream[A](dstream, encoder, f(cfg))

  def checkpoint(fd: FiniteDuration): SparkAvroDStream[A] = updateConfig(_.withCheckpointDuration(fd))

  def transform[B: ClassTag](f: RDD[A] => RDD[B], encoder: AvroEncoder[B]): SparkAvroDStream[B] =
    new SparkAvroDStream[B](dstream.transform(f), encoder, cfg)

  def transform(f: RDD[A] => RDD[A])(implicit ev: ClassTag[A]): SparkAvroDStream[A] =
    new SparkAvroDStream[A](dstream.transform(f), encoder, cfg)

  def circe(path: String)(implicit enc: JsonEncoder[A]): EndMark =
    persist.circe[A](dstream.checkpoint(params.checkpointDuration))(params.pathBuilder(path))

  def avro(path: String): EndMark =
    persist.avro(dstream.checkpoint(params.checkpointDuration), encoder)(params.pathBuilder(path))

  def jackson(path: String): EndMark =
    persist.jackson(dstream.checkpoint(params.checkpointDuration), encoder)(params.pathBuilder(path))
}

object SparkAvroDStream {

  implicit def getterSparkAvroDStream[A]: Getter[SparkAvroDStream[A], DStream[A]] =
    (s: SparkAvroDStream[A]) => s.dstream
}
