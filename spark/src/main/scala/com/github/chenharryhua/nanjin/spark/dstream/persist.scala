package com.github.chenharryhua.nanjin.spark.dstream

import cats.effect.{Blocker, ConcurrentEffect, ContextShift}
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}
import com.github.chenharryhua.nanjin.spark.persist.{RddAvroFileHoarder, RddFileHoarder}
import com.sksamuel.avro4s.{Encoder => AvroEncoder}
import io.circe.{Encoder => JsonEncoder}
import org.apache.spark.streaming.dstream.DStream
import cats.effect.syntax.all._

private[dstream] object persist {

  def circe[F[_]: ConcurrentEffect: ContextShift, A: JsonEncoder](ds: DStream[A], blocker: Blocker)(
    pathBuilder: NJTimestamp => String): Unit =
    ds.foreachRDD { (rdd, time) =>
      val path   = pathBuilder(NJTimestamp(time.milliseconds))
      val action = new RddFileHoarder[F, A](rdd).circe(path).folder.append
      action.run(blocker).toIO.unsafeRunSync()
    }

  def jackson[F[_]: ConcurrentEffect: ContextShift, A](ds: DStream[A], encoder: AvroEncoder[A], blocker: Blocker)(
    pathBuilder: NJTimestamp => String): Unit =
    ds.foreachRDD { (rdd, time) =>
      val path   = pathBuilder(NJTimestamp(time.milliseconds))
      val action = new RddAvroFileHoarder[F, A](rdd, encoder).jackson(path).folder.append
      action.run(blocker).toIO.unsafeRunSync()
    }

  def avro[F[_]: ConcurrentEffect: ContextShift, A](ds: DStream[A], encoder: AvroEncoder[A], blocker: Blocker)(
    pathBuilder: NJTimestamp => String): Unit =
    ds.foreachRDD { (rdd, time) =>
      val path   = pathBuilder(NJTimestamp(time.milliseconds))
      val action = new RddAvroFileHoarder[F, A](rdd, encoder).avro(path).folder.append
      action.run(blocker).toIO.unsafeRunSync()
    }
}
