package com.github.chenharryhua.nanjin.spark.dstream

import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import io.circe.{Encoder => JsonEncoder}
import monocle.Getter
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

final class SparkDStream[A](val dstream: DStream[A], cfg: SDConfig) extends Serializable {
  val params: SDParams = cfg.evalConfig

  def transform[B: ClassTag](f: RDD[A] => RDD[B]): SparkDStream[B] = new SparkDStream[B](dstream.transform(f), cfg)

  def circe(path: String)(implicit enc: JsonEncoder[A]): EndMark = persist.circe[A](dstream)(params.pathBuilder(path))
}

object SparkDStream {

  implicit def getterSparkDStream[A]: Getter[SparkDStream[A], DStream[A]] =
    (s: SparkDStream[A]) => s.dstream
}

final class SparkAvroDStream[A](val dstream: DStream[A], encoder: AvroEncoder[A], cfg: SDConfig) extends Serializable {
  val params: SDParams = cfg.evalConfig

  def transform[B: ClassTag](f: RDD[A] => RDD[B], encoder: AvroEncoder[B]): SparkAvroDStream[B] =
    new SparkAvroDStream[B](dstream.transform(f), encoder, cfg)

  def transform(f: RDD[A] => RDD[A])(implicit ev: ClassTag[A]): SparkAvroDStream[A] =
    new SparkAvroDStream[A](dstream.transform(f), encoder, cfg)

  def circe(path: String)(implicit enc: JsonEncoder[A]): EndMark = persist.circe[A](dstream)(params.pathBuilder(path))
  def avro(path: String): EndMark                                = persist.avro(dstream, encoder)(params.pathBuilder(path))
  def jackson(path: String): EndMark                             = persist.jackson(dstream, encoder)(params.pathBuilder(path))
}

object SparkAvroDStream {

  implicit def getterSparkAvroDStream[A]: Getter[SparkAvroDStream[A], DStream[A]] =
    (s: SparkAvroDStream[A]) => s.dstream
}
