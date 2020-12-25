package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.{akkaSinks, KafkaTopic}
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.kafka._
import com.landoop.transportation.nyc.trip.yellow.trip_record
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import mtest.spark.{akkaSystem, blocker, contextShift, ctx, sparkSession}
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

object SparkExtTestData {
  final case class Foo(a: Int, b: String)
  val list: List[Foo] = List(Foo(1, "a"), null.asInstanceOf[Foo], Foo(3, "c"))
}

class SparkExtTest extends AnyFunSuite {
  implicit val te: TypedEncoder[trip_record]                      = shapeless.cachedImplicit
  implicit val te2: TypedEncoder[OptionalKV[String, trip_record]] = shapeless.cachedImplicit
  implicit val te3: TypedEncoder[SparkExtTestData.Foo]            = shapeless.cachedImplicit

  val topic: KafkaTopic[IO, String, trip_record] =
    ctx.topic[String, trip_record]("nyc_yellow_taxi_trip_data")

  val ate: AvroTypedEncoder[OptionalKV[String, trip_record]] = OptionalKV.ate(topic.topicDef)

  test("stream") {
    topic.sparKafka.fromKafka
      .flatMap(_.crDS.typedDataset.stream[IO].compile.drain)
      .unsafeRunSync
  }
  test("source") {
    topic
      .sparKafka(range)
      .fromKafka
      .flatMap(
        _.crDS.ascendTimestamp.typedDataset
          .source[IO]
          .map(println)
          .take(10)
          .runWith(akkaSinks.ignore[IO]))
      .unsafeRunSync
  }

  test("sparKafka rdd deal with primitive null ") {
    val rdd: RDD[Int] =
      sparkSession.sparkContext.parallelize(List(1, null.asInstanceOf[Int], 3))
    assert(rdd.dismissNulls.collect().toList == List(1, 0, 3))
    assert(rdd.numOfNulls == 0)
  }

  test("sparKafka rdd remove null object") {
    import SparkExtTestData._
    val rdd: RDD[Foo] = sparkSession.sparkContext.parallelize(list)
    assert(rdd.dismissNulls.collect().toList == List(Foo(1, "a"), Foo(3, "c")))
    assert(rdd.numOfNulls == 1)
  }
  test("sparKafka typed dataset deal with primitive null ") {
    val tds = TypedDataset.create[Int](List(1, null.asInstanceOf[Int], 3))
    assert(tds.dismissNulls.collect[IO]().unsafeRunSync().toList == List(1, 0, 3))
    assert(tds.numOfNulls[IO].unsafeRunSync() == 0)
  }

  test("sparKafka typed dataset remove null object") {
    import SparkExtTestData._
    val tds = TypedDataset.create[Foo](sparkSession.sparkContext.parallelize(list))
    assert(tds.dismissNulls.collect[IO]().unsafeRunSync().toList == List(Foo(1, "a"), Foo(3, "c")))
    assert(tds.numOfNulls[IO].unsafeRunSync() == 1)
  }
  test("save syntax") {
    import SparkExtTestData._
    val ate = AvroTypedEncoder[Foo]
    val rdd = sparkSession.sparkContext.parallelize(list.flatMap(Option(_)))
    val tds = rdd.typedDataset(ate)
    rdd
      .save[IO](ate.avroCodec.avroEncoder)
      .avro("./data/test/spark/sytax/avro")
      .run(blocker)
      .unsafeRunSync()
    tds.save[IO].parquet("./data/test/spark/syntax/parquet").run(blocker).unsafeRunSync()
  }
}
