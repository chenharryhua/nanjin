package com.github.chenharryhua.nanjin.spark.persist

import cats.Show
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.SchematizedEncoder
import com.github.chenharryhua.nanjin.spark.injection.*
import frameless.TypedEncoder
import kantan.csv.RowEncoder
import kantan.csv.generic.*
import kantan.csv.java8.*
import mtest.spark.*
import org.apache.spark.rdd.RDD

import java.time.{Instant, LocalDate}
import java.time.temporal.ChronoUnit
import scala.util.Random

final case class Tablet(a: Int, b: Long, c: Float, d: LocalDate, e: Instant, f: String)

object Tablet {
  val avroCodec: AvroCodec[Tablet] = AvroCodec[Tablet]
  implicit val te: TypedEncoder[Tablet] = shapeless.cachedImplicit
  val ate: SchematizedEncoder[Tablet] = SchematizedEncoder(avroCodec)
  implicit val re: RowEncoder[Tablet] = shapeless.cachedImplicit
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
        Instant.now.truncatedTo(ChronoUnit.MILLIS),
        """a_"b?c\n*\r'|,"""))

  val rdd: RDD[Tablet] = sparkSession.sparkContext.parallelize(data)

}
