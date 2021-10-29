package mtest.spark.kafka

import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.spark.kafka.NJProducerRecord
import io.circe.generic.auto._
import mtest.spark.akkaSystem
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random
import cats.effect.unsafe.implicits.global

class KafkaDownloadTest extends AnyFunSuite {
  val topic = sparKafka.topic[Int, Int]("spark.kafka.download")
  val now   = NJTimestamp.now().milliseconds
  val rand  = Random.nextInt(10)

  // format: off
  val data =
    List(                                                      // #0
      NJProducerRecord(1, rand + 1).withTimestamp(now + 1000), // #1
      NJProducerRecord(2, rand + 2).withTimestamp(now + 2000), // #2
      NJProducerRecord(3, rand + 3).withTimestamp(now + 3000), // #3
      NJProducerRecord(4, rand + 4).withTimestamp(now + 4000), // #4
      NJProducerRecord(5, rand + 5).withTimestamp(now + 5000)  // #5
    )                                                          // #6
  // format: on

  topic
    .prRdd(data)
    .uploadByChunk
    .updateProducer(_.withClientId("kafka.download.test"))
    .run
    .compile
    .drain
    .unsafeRunSync()

  val root = "./data/test/spark/kafka/kafka_download/"

  test("download - whole topic") {
    val path = root + "whole_topic/download.avro"
    val dr   = NJDateTimeRange(sydneyTime)
    topic.withTimeRange(dr).download(akkaSystem).avro(path).run.compile.drain.unsafeRunSync()
    assert(topic.load.avro(path).map(_.dataset.count()).unsafeRunSync() >= 5)
  }

  test("download - from #1 to #5") {
    val path = root + "15.jackson.json"
    val dr   = NJDateTimeRange(sydneyTime).withStartTime(now).withEndTime(now + 5000)
    topic.withTimeRange(dr).download(akkaSystem).jackson(path).run.compile.drain.unsafeRunSync()

    val res      = topic.load.jackson(path).map(_.dataset.collect().map(_.value.get).toSet).unsafeRunSync()
    val expected = Set(rand + 1, rand + 2, rand + 3, rand + 4)
    assert(res == expected)
  }

  test("download - from #0 to less #2") {
    val path = root + "0less2.snappy.parquet"
    val dr   = NJDateTimeRange(sydneyTime).withStartTime(now).withEndTime(now + 1200)
    topic.withTimeRange(dr).download(akkaSystem).parquet(path).snappy.run.compile.drain.unsafeRunSync()

    val res      = topic.load.parquet(path).map(_.dataset.collect().map(_.value.get).toSet).unsafeRunSync()
    val expected = Set(rand + 1)
    assert(res == expected)
  }
  test("download - from #0 to #2") {
    val path = root + "02.bzip2.avro"
    val dr   = NJDateTimeRange(sydneyTime).withStartTime(now).withEndTime(now + 2000)
    topic.withTimeRange(dr).download(akkaSystem).avro(path).bzip2.run.compile.drain.unsafeRunSync()

    val res      = topic.load.avro(path).map(_.dataset.collect().map(_.value.get).toSet).unsafeRunSync()
    val expected = Set(rand + 1)
    assert(res == expected)
  }
  test("download - from #3 to #6") {
    val path = root + "36.circe.json.gz"
    val dr   = NJDateTimeRange(sydneyTime).withStartTime(now + 3000)
    topic.withTimeRange(dr).download(akkaSystem).circe(path).gzip.run.compile.drain.unsafeRunSync()

    val res      = topic.load.circe(path).unsafeRunSync().dataset.collect().map(_.value.get).toSet
    val expected = Set(rand + 3, rand + 4, rand + 5)
    assert(res == expected)
  }
  test("download - from #2.5 to #3.5") {
    val path = root + "between2535.parquet"
    val dr   = NJDateTimeRange(sydneyTime).withStartTime(now + 2500).withEndTime(now + 3500)
    topic.withTimeRange(dr).download(akkaSystem).parquet(path).uncompress.run.compile.drain.unsafeRunSync()

    val res      = topic.load.parquet(path).map(_.dataset.collect().map(_.value.get).toSet).unsafeRunSync()
    val expected = Set(rand + 3)
    assert(res == expected)
  }
}
