package com.github.chenharryhua.nanjin.spark.dstream

import cats.effect.{Blocker, ConcurrentEffect, ContextShift}
import com.github.chenharryhua.nanjin.spark.persist.{RddAvroFileHoarder, RddFileHoarder}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import io.circe.Encoder
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import scala.reflect.ClassTag

final class SparkDStream[F[_], A](val dstream: DStream[A], encoder: AvroEncoder[A]) {

  // val streamingContext = new StreamingContext(sparkSession.sparkContext, Duration(10000))

  def jackson(
    blocker: Blocker)(implicit F: ConcurrentEffect[F], cs: ContextShift[F], en: Encoder[A], tag: ClassTag[A]) = {
    val a = 0
    dstream.foreachRDD(rdd =>
      F.toIO(new RddAvroFileHoarder(rdd.coalesce(1), encoder).circe("./data/dstream").append.run(blocker))
        .unsafeRunSync())
  }
}
