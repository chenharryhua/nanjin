package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.kafka._
import com.landoop.transportation.nyc.trip.yellow.trip_record
import frameless.{TypedDataset, TypedEncoder}
import io.circe.generic.auto._
import mtest.spark.sparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

object SparkExtTestData {
  final case class Foo(a: Int, b: String)
  val list: List[Foo] = List(Foo(1, "a"), null.asInstanceOf[Foo], Foo(3, "c"))
}

class SparkExtTest extends AnyFunSuite {
  implicit val te: TypedEncoder[trip_record]                            = shapeless.cachedImplicit
  implicit val te2: TypedEncoder[NJConsumerRecord[String, trip_record]] = shapeless.cachedImplicit
  implicit val te3: TypedEncoder[SparkExtTestData.Foo]                  = shapeless.cachedImplicit

  implicit val ss: SparkSession = sparkSession

  val topic: KafkaTopic[IO, String, trip_record] =
    ctx.topic[String, trip_record]("nyc_yellow_taxi_trip_data")

  val ate: AvroTypedEncoder[NJConsumerRecord[String, trip_record]] = NJConsumerRecord.ate(topic.topicDef)

  test("stream") {
    sparKafka.topic(topic).fromKafka.flatMap(_.crDS.typedDataset.stream[IO].compile.drain).unsafeRunSync
  }
  /*
  test("source") {
    sparKafka
      .topic(topic)
      .withStartTime("2012-10-26")
      .withEndTime("2012-10-28")
      .fromKafka.flatMap(
      _.crDS
      .ascendTimestamp
      .typedDataset
      .source[IO]
      .map(println)
      .take(10)
      .runWith(stages.ignore[IO])
      ).unsafeRunSync
  } */

  test("sparKafka rdd deal with primitive null ") {
    val rdd: RDD[Int] = sparkSession.sparkContext.parallelize(List(1, null.asInstanceOf[Int], 3))
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
    assert(tds.dismissNulls.dataset.collect().toList == List(1, 0, 3))
    assert(tds.numOfNulls == 0)
  }

  test("sparKafka typed dataset remove null object") {
    import SparkExtTestData._
    val tds = TypedDataset.create[Foo](sparkSession.sparkContext.parallelize(list))
    assert(tds.dismissNulls.dataset.collect().toList == List(Foo(1, "a"), Foo(3, "c")))
    assert(tds.numOfNulls == 1)
  }
  test("save syntax") {
    import SparkExtTestData._
    val ate = AvroTypedEncoder[Foo]
    val rdd = sparkSession.sparkContext.parallelize(list.flatMap(Option(_)))
    rdd.save[IO](ate.avroCodec.avroEncoder).avro("./data/test/spark/sytax/rdd/avro").folder.run.unsafeRunSync()
    rdd.save[IO].circe("./data/test/spark/sytax/rdd/circe").folder.run.unsafeRunSync()
    val ds = TypedDataset.create(rdd)
    ds.save[IO](ate.avroCodec.avroEncoder).parquet("./data/test/spark/sytax/ds/parquet").folder.run.unsafeRunSync()
    ds.save[IO].json("./data/test/spark/sytax/ds/json").run.unsafeRunSync()
  }
}
