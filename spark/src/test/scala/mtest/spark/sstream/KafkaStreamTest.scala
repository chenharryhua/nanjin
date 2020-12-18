package mtest.spark.sstream

import better.files._
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark.kafka.{NJProducerRecord, _}
import frameless.TypedEncoder
import mtest.spark.persist.{Rooster, RoosterData}
import mtest.spark.{contextShift, ctx, sparkSession, timer}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.concurrent.duration._
import scala.util.Random

class KafkaStreamTest extends AnyFunSuite {

  val root = "./data/test/spark/sstream/"

  val data = RoosterData.rdd.map(x => NJProducerRecord(Random.nextInt(), x))

  implicit val te: TypedEncoder[OptionalKV[Int, Int]] = shapeless.cachedImplicit

  test("console sink") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("sstream.console.rooster"), Rooster.avroCodec).in(ctx)
    val ss = rooster.sparKafka.sstream
      .map(x => x.newValue(x.value.map(_.index + 1)))
      .flatMap(x => x.value.map(_ => x))
      .sstream
      .withParamUpdate(
        _.withProcessingTimeTrigger(
          1000).withJson.withUpdate.withComplete.withAppend.failOnDataLoss.ignoreDataLoss
          .withProgressInterval(1000)
          .withProgressInterval(1.seconds))
      .map(List(_))
      .filter(_ => true)
      .flatMap(identity)
      .transform(identity)
      .consoleSink
      .showProgress

    val upload = rooster.sparKafka.prRdd(data).batch(1).interval(1000).upload.delayBy(1.second)

    ss.concurrently(upload).interruptAfter(6.seconds).compile.drain.unsafeRunSync()
  }

  test("file sink") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("sstream.file.rooster"), Rooster.avroCodec).in(ctx)
    val ss = rooster.sparKafka.sstream.sstream
      .withParamUpdate(_.withProcessingTimeTrigger(500).withAvro)
      .fileSink(root + "fileSink")
      .withOptions(identity)
      .queryStream
      .interruptAfter(5.seconds)
    val upload = rooster.sparKafka
      .prRdd(data)
      .interval(1.second)
      .timeLimit(2000)
      .timeLimit(2.minute)
      .recordsLimit(10)
      .upload
      .delayBy(1.second)
    ss.concurrently(upload).compile.drain.unsafeRunSync()
  }

  test("date partition sink") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("sstream.datepatition.rooster"), Rooster.avroCodec).in(ctx)

    val path = root + "date_partition"

    val ss = rooster
      .sparKafka(sydneyTime)
      .sstream
      .withParamUpdate(_.withProcessingTimeTrigger(500).withParquet)
      .datePartitionFileSink(path)
      .queryStream
      .interruptAfter(10.seconds)
    val upload = rooster.sparKafka.prRdd(data).upload.delayBy(1.second)
    ss.concurrently(upload).compile.drain.unsafeRunSync()
    val l = NJTimestamp(Instant.now()).`Year=yyyy/Month=mm/Day=dd`(sydneyTime)
    assert(!File(path + "/" + l).isEmpty)
  }

  test("memory sink - kafka timestamp") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("sstream.memory.rooster"), Rooster.avroCodec).in(ctx)
    val ss = rooster.sparKafka.sstream.sstream
      .withParamUpdate(_.withProcessingTimeTrigger(500).withAvro)
      .memorySink("kafka")
      .queryStream

    val upload = rooster.sparKafka.prRdd(data).batch(1).interval(1.second).upload.delayBy(1.second)
    ss.concurrently(upload).interruptAfter(5.seconds).compile.drain.unsafeRunSync()
    import sparkSession.implicits._
    val now = Instant.now().getEpochSecond * 1000 //to millisecond
    sparkSession
      .sql("select timestamp from kafka")
      .as[Long]
      .collect()
      .foreach(t => assert(Math.abs(now - t) < 5000))
  }
}
