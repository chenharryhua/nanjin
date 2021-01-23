package com.github.chenharryhua.nanjin.spark.dstream

import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import io.circe.Encoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

final class SparkDStream[A](ds: DStream[A], cfg: SDConfig) extends Serializable {
  val params: SDParams = cfg.evalConfig

  def transform[B: ClassTag](f: RDD[A] => RDD[B]): SparkDStream[B] = new SparkDStream[B](ds.transform(f), cfg)

  def circe(path: String)(implicit enc: Encoder[A]): EndMark = persist.circe[A](ds)(params.pathBuilder(path))
}

final class SparkAvroDStream[A](ds: DStream[A], encoder: AvroEncoder[A], cfg: SDConfig) extends Serializable {
  val params: SDParams = cfg.evalConfig

  def transform[B: ClassTag](f: RDD[A] => RDD[B], encoder: AvroEncoder[B]): SparkAvroDStream[B] =
    new SparkAvroDStream[B](ds.transform(f), encoder, cfg)

  def transform(f: RDD[A] => RDD[A])(implicit ev: ClassTag[A]): SparkAvroDStream[A] =
    new SparkAvroDStream[A](ds.transform(f), encoder, cfg)

  def circe(path: String)(implicit enc: Encoder[A]): EndMark = persist.circe[A](ds)(params.pathBuilder(path))
  def avro(path: String): EndMark                            = persist.avro(ds, encoder)(params.pathBuilder(path))
  def jackson(path: String): EndMark                         = persist.jackson(ds, encoder)(params.pathBuilder(path))
}
