package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.{akkaSinks, KafkaTopic, TopicName}
import com.github.chenharryhua.nanjin.spark.kafka._
import com.landoop.transportation.nyc.trip.yellow.trip_record
import frameless.cats.implicits._
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.spark._
import frameless.TypedDataset

object SparkExtTestData {
  final case class Foo(a: Int, b: String)
  val list: List[Foo] = List(Foo(1, "a"), null.asInstanceOf[Foo], Foo(3, "c"))
}

class SparkExtTest extends AnyFunSuite {

  val topic: KafkaTopic[IO, String, trip_record] =
    ctx.topic[String, trip_record](TopicName("nyc_yellow_taxi_trip_data"))
  test("stream") {
    topic.sparKafka.fromKafka.flatMap(_.sorted.stream.compile.drain).unsafeRunSync
  }
  test("source") {
    topic.sparKafka.fromKafka
      .flatMap(_.sorted.source.map(println).take(10).runWith(akkaSinks.ignore[IO]))
      .unsafeRunSync
  }

  test("rdd remove primitive null ") {
    val rdd: RDD[Int] =
      sparkSession.sparkContext.parallelize(List(1, null.asInstanceOf[Int], 3))
    assert(rdd.validRecords.collect().toList == List(1, 0, 3))
  }

  test("rdd remove object null ") {
    import SparkExtTestData._
    val rdd: RDD[Foo] = sparkSession.sparkContext.parallelize(list)
    assert(rdd.validRecords.collect().toList == List(Foo(1, "a"), Foo(3, "c")))
  }
  test("typed dataset remove primitive null ") {
    val tds = TypedDataset.create[Int](List(1, null.asInstanceOf[Int], 3))
    assert(tds.validRecords.collect[IO]().unsafeRunSync().toList == List(1, 0, 3))
  }

  test("typed dataset remove object null ") {
    import SparkExtTestData._
    val tds = TypedDataset.create[Foo](sparkSession.sparkContext.parallelize(list))
    assert(tds.validRecords.collect[IO]().unsafeRunSync().toList == List(Foo(1, "a"), Foo(3, "c")))
  }

}
