package com.github.chenharryhua.nanjin.spark.dstream

import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import io.circe.Encoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

sealed class SparkDStream[A](ds: DStream[A]) extends Serializable {

  protected def pathBuilder(path: String)(ts: NJTimestamp): String =
    if (path.endsWith("/"))
      s"$path${ts.`Year=yyyy/Month=mm/Day=dd`(sydneyTime)}"
    else
      s"$path/${ts.`Year=yyyy/Month=mm/Day=dd`(sydneyTime)}"

  def transform[B: ClassTag](f: RDD[A] => RDD[B]): SparkDStream[B] = new SparkDStream[B](ds.transform(f))

  def circe(path: String)(implicit enc: Encoder[A]): Unit =
    persist.circe[A](ds)(pathBuilder(path))
}

final class SparkAvroDStream[A](ds: DStream[A], encoder: AvroEncoder[A]) extends SparkDStream[A](ds) {

  def avro(path: String): Unit    = persist.avro(ds, encoder)(pathBuilder(path))
  def jackson(path: String): Unit = persist.jackson(ds, encoder)(pathBuilder(path))
}
