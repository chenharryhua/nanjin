package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.datetime.zones.sydneyTime
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.terminals.NJPath
import com.landoop.transportation.nyc.trip.yellow.trip_record
import eu.timepit.refined.auto.*
import frameless.TypedEncoder
import io.circe.generic.auto.*
import mtest.spark.sparkSession
import org.apache.spark.sql.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant

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

  val ate: AvroTypedEncoder[NJConsumerRecord[String, trip_record]] = AvroTypedEncoder(topic.topicDef)

  test("stream") {
    val dr   = NJDateTimeRange(sydneyTime).withEndTime(Instant.now)
    val data = sparKafka.topic(topic).fromKafka(dr)
    val res = for {
      a <- data.output.stream(100).compile.toList
      b <- data.asTable.output.stream(100).compile.toList
    } yield assert(a.toSet === b.toSet)
    res.unsafeRunSync()
  }

  test("save syntax") {
    import SparkExtTestData.*
    import sparkSession.implicits.*
    val ate = AvroTypedEncoder[Foo]
    val rdd = sparkSession.sparkContext.parallelize(list.flatMap(Option(_)))
    IO(rdd)
      .output(ate.avroCodec.avroEncoder)
      .avro(NJPath("./data/test/spark/sytax/rdd/avro"))
      .run
      .unsafeRunSync()
    IO(rdd).output.circe(NJPath("./data/test/spark/sytax/rdd/circe")).run.unsafeRunSync()
    val ds = sparkSession.createDataset(rdd)
    IO(ds.rdd)
      .output(ate.avroCodec.avroEncoder)
      .parquet(NJPath("./data/test/spark/sytax/ds/parquet"))
      .run
      .unsafeRunSync()
  }
}
