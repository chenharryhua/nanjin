package com.github.chenharryhua.nanjin.spark.dstream

import cats.effect.{Blocker, ConcurrentEffect, ContextShift}
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}
import com.github.chenharryhua.nanjin.spark.persist.{RddAvroFileHoarder, RddFileHoarder}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import io.circe.Encoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

final class SparkDStream[F[_], A](val dstream: DStream[A], encoder: AvroEncoder[A]) {
  val sparkSession: SparkSession = null

  val sc = new StreamingContext(sparkSession.sparkContext, Duration(120000))
  sc.checkpoint("./data/cp")

  def jackson(
    blocker: Blocker)(implicit F: ConcurrentEffect[F], cs: ContextShift[F], en: Encoder[A], tag: ClassTag[A]) =
    dstream.foreachRDD { (rdd, time) =>
      val nj = NJTimestamp(time.milliseconds).`Year=yyyy/Month=mm/Day=dd`(sydneyTime)
      F.toIO(new RddAvroFileHoarder(rdd.coalesce(1), encoder).circe(s"./data/dstream/$nj").folder.append.run(blocker))
        .unsafeRunSync()
    }
}
