package com.github.chenharryhua.nanjin.spark.persist

import cats.effect.{Blocker, Concurrent, ContextShift}
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.RddExt
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scalapb.GeneratedMessage

import scala.reflect.ClassTag

final class SaveProtobuf[F[_], A](rdd: RDD[A], codec: AvroCodec[A], cfg: HoarderConfig)
    extends Serializable {

  val params: HoarderParams = cfg.evalConfig

  def run(blocker: Blocker)(implicit
    F: Concurrent[F],
    cs: ContextShift[F],
    ss: SparkSession,
    enc: A <:< GeneratedMessage,
    tag: ClassTag[A]): F[Unit] =
    rdd
      .map(codec.idConversion)
      .stream[F]
      .through(fileSink[F](blocker)(ss).protobuf[A](params.outPath))
      .compile
      .drain
}
