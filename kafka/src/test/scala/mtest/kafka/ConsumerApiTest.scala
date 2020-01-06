package mtest.kafka

import cats.derived.auto.show._
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka._
import io.circe.generic.auto._
import org.scalatest.funsuite.AnyFunSuite
import cats.implicits._
import cats.effect.IO

class ConsumerApiTest extends AnyFunSuite {

  val nyc_taxi_trip: TopicDef[Array[Byte], trip_record] =
    TopicDef[Array[Byte], trip_record]("nyc_yellow_taxi_trip_data")

  test("should be able to retrieve messages without error") {
    nyc_taxi_trip
      .in(ctx)
      .consumerResource
      .use(c => c.numOfRecords >> c.retrieveFirstRecords >> c.retrieveLastRecords)
      .unsafeRunSync()
  }
  test("range for non-exist topic") {
    val topic = ctx.topic[Int, Int]("non-exist")
    topic.consumerResource
      .use(_.offsetRangeFor(NJDateTimeRange.infinite).map(println))
      .unsafeRunSync()
  }
}
