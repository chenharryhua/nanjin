package com.github.chenharryhua.nanjin.spark.saver

import cats.effect.{Blocker, Sync}
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.{fileSink, RddExt}
import io.circe.{Decoder => JsonDecoder, Encoder => JsonEncoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import io.circe.parser.decode
import scala.reflect.ClassTag
import cats.effect.ContextShift

/**
  * may not load what be saved if coproduct type involved
  */
final class CirceLoader[A: ClassTag](ss: SparkSession, decoder: JsonDecoder[A])
    extends Serializable {
  implicit private val dec: JsonDecoder[A] = decoder

  def load(pathStr: String): RDD[A] =
    ss.sparkContext
      .textFile(pathStr)
      .map(decode[A](_) match {
        case Left(ex) => throw ex
        case Right(r) => r
      })

}

final class CirceSaver[F[_], A: ClassTag](
  rdd: RDD[A],
  encoder: JsonEncoder[A],
  codec: NJAvroCodec[A],
  ss: SparkSession)
    extends Serializable {

  implicit val enc: JsonEncoder[A] = encoder

  def multi(pathStr: String)(implicit F: Sync[F]): F[Unit] =
    F.delay(rdd.map(a => encoder(codec.idConversion(a)).noSpaces).saveAsTextFile(pathStr))

  def single(pathStr: String, blocker: Blocker)(implicit F: Sync[F], cs: ContextShift[F]): F[Unit] =
    rdd
      .map(codec.idConversion)
      .stream[F]
      .through(fileSink[F](blocker)(ss).circe(pathStr))
      .compile
      .drain
}
