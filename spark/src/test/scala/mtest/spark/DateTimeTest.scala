package mtest.spark

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, ZoneId}

import cats.effect.IO
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import com.sksamuel.avro4s.Encoder
import frameless.TypedEncoder
import frameless.cats.implicits._
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

object DateTimeTestData {

  final case class Rooster(
    ld: LocalDate,
    // ldt: LocalDateTime,
    // zdt: ZonedDateTime,
    // odt: OffsetDateTime,
    now: Instant,
    dt: Date,
    ts: Timestamp)
  implicit val zoneId: ZoneId                 = sydneyTime
  implicit val encoder: TypedEncoder[Rooster] = shapeless.cachedImplicit
  implicit val avro: Encoder[Rooster]         = shapeless.cachedImplicit

  val data: Rooster = Rooster(
    LocalDate.now(),
    // LocalDateTime.now(),
    //  ZonedDateTime.now(),
    // OffsetDateTime.now,
    Instant.now(),
    Date.valueOf(LocalDate.now),
    Timestamp.from(Instant.now())
  )

  val rdd: RDD[Rooster] = sparkSession.sparkContext.parallelize(List(data))

}

class DateTimeTest extends AnyFunSuite {
  import DateTimeTestData._
  test("data-time") {
    assert(rdd.typedDataset.collect[IO].unsafeRunSync().head == data)
  }
}
