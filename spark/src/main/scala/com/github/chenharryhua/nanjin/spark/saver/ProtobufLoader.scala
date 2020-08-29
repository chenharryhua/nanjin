package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Concurrent, ContextShift}
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import com.google.protobuf.CodedInputStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import scala.reflect.ClassTag

final class ProtobufLoader[A <: GeneratedMessage: ClassTag](
  ss: SparkSession,
  decoder: GeneratedMessageCompanion[A])
    extends Serializable {

  def load(pathStr: String): RDD[A] =
    ss.sparkContext
      .binaryFiles(pathStr)
      .mapPartitions(_.flatMap {
        case (_, pds) =>
          val is   = pds.open()
          val cis  = CodedInputStream.newInstance(is)
          val iter = decoder.parseDelimitedFrom(cis)
          new Iterator[A] {
            override def hasNext: Boolean = if (iter.isDefined) true else { is.close(); false }
            override def next(): A        = iter.get
          }
      })
}

final class ProtobufSaver[F[_], A](rdd: RDD[A], ss: SparkSession)(implicit
  enc: A <:< GeneratedMessage)
    extends Serializable {

  def single(pathStr: String, blocker: Blocker)(implicit
    F: Concurrent[F],
    cs: ContextShift[F]): F[Unit] =
    rdd.stream[F].through(fileSink[F](blocker)(ss).protobuf[A](pathStr)).compile.drain
}
