package com.github.chenharryhua.nanjin.spark.dstream

import cats.data.Reader
import cats.effect.{Blocker, ConcurrentEffect, ContextShift}
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import io.circe.Encoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

sealed class SparkDStream[F[_], A](ds: DStream[A]) extends Serializable {

  protected def pathBuilder(path: String)(ts: NJTimestamp): String =
    if (path.endsWith("/"))
      s"$path${ts.`Year=yyyy/Month=mm/Day=dd`(sydneyTime)}"
    else
      s"$path/${ts.`Year=yyyy/Month=mm/Day=dd`(sydneyTime)}"

  def transform[B: ClassTag](f: RDD[A] => RDD[B]): SparkDStream[F, B] = new SparkDStream[F, B](ds.transform(f)) 

  def circe(path: String)(implicit F: ConcurrentEffect[F], cs: ContextShift[F], en: Encoder[A]): Reader[Blocker, Unit] =
    Reader((blocker: Blocker) => persist.circe[F, A](ds, blocker)(pathBuilder(path)))
}

final class SparkAvroDStream[F[_], A](ds: DStream[A], encoder: AvroEncoder[A]) extends SparkDStream[F, A](ds) {

  def jackson(
    path: String)(implicit F: ConcurrentEffect[F], cs: ContextShift[F], en: Encoder[A]): Reader[Blocker, Unit] =
    Reader((blocker: Blocker) => persist.jackson[F, A](ds, encoder, blocker)(pathBuilder(path)))

  def avro(path: String)(implicit F: ConcurrentEffect[F], cs: ContextShift[F], en: Encoder[A]): Reader[Blocker, Unit] =
    Reader((blocker: Blocker) => persist.avro[F, A](ds, encoder, blocker)(pathBuilder(path)))

}
