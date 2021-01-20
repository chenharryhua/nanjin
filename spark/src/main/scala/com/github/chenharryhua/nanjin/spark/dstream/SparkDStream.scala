package com.github.chenharryhua.nanjin.spark.dstream

import cats.effect.{Blocker, ConcurrentEffect, ContextShift}
import com.github.chenharryhua.nanjin.spark.persist.{RddAvroFileHoarder, RddFileHoarder}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

final class SparkDStream[F[_], A](val dstream: DStream[A], encoder: AvroEncoder[A]) {

  // val streamingContext = new StreamingContext(sparkSession.sparkContext, Duration(10000))

  def jackson(blocker: Blocker)(implicit F: ConcurrentEffect[F], cs: ContextShift[F]) =
    dstream.foreachRDD(rdd =>
      F.toIO(new RddAvroFileHoarder(rdd, encoder).jackson("./data/dstream").append.run(blocker)).unsafeRunSync())
}
