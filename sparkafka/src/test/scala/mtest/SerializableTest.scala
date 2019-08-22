package mtest

import java.time.LocalDateTime

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.sparkafka.SharedVariable
import org.scalatest.FunSuite
import frameless.cats.implicits._

class SerializableTest extends FunSuite with SparkafkaSyntax {
  test("serizable") {
    val end   = LocalDateTime.now()
    val start = end.minusYears(1)
    val topic: SharedVariable[KafkaTopic[IO, Int, trip_record]] =
      SharedVariable(ctx.topic[Int, trip_record]("nyc_yellow_taxi_trip_data"))
    spark.use { implicit s =>
      // implicit val x = s
      topic.dataset(start, end).flatMap(_.show[IO]())
    }.map(println).unsafeRunSync
  }
}
