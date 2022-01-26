package com.github.chenharryhua.nanjin.spark.persist

import java.time.{Instant, LocalDate}
import cats.Show
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.injection.*
import frameless.TypedEncoder
import kantan.csv.RowEncoder
import kantan.csv.java8.*
import kantan.csv.generic.*
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import mtest.spark.*

import java.time.temporal.ChronoUnit
import scala.util.Random

final case class Tablet(a: Int, b: Long, c: Float, d: LocalDate, e: Instant)

object Tablet {
  val avroCodec: AvroCodec[Tablet]      = AvroCodec[Tablet]
  implicit val te: TypedEncoder[Tablet] = shapeless.cachedImplicit
  val ate: AvroTypedEncoder[Tablet]     = AvroTypedEncoder(avroCodec)
  implicit val re: RowEncoder[Tablet]   = shapeless.cachedImplicit
  implicit val showTablet: Show[Tablet] = _.toString
}

object TabletData {

  val data: List[Tablet] =
    List.fill(10000)(
      Tablet(
        Random.nextInt(),
        Random.nextLong(),
        Random.nextFloat(),
        LocalDate.now,
        Instant.now.truncatedTo(ChronoUnit.MILLIS)))

  val rdd: RDD[Tablet]    = sparkSession.sparkContext.parallelize(data)
  val ds: Dataset[Tablet] = Tablet.ate.normalize(rdd, sparkSession)

}
