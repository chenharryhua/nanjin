package mtest.spark.sstream

import better.files._
import cats.syntax.all._
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark.kafka.{NJProducerRecord, _}
import frameless.TypedEncoder
import mtest.spark.persist.{Rooster, RoosterData}
import mtest.spark.{contextShift, ctx, sparKafka, sparkSession, timer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.concurrent.duration._
import scala.util.Random

@DoNotDiscover
class KafkaStreamTest extends AnyFunSuite {

  val root = "./data/test/spark/sstream/"

  val roosterTopic: TopicDef[Int, Rooster] =
    TopicDef[Int, Rooster](TopicName("sstream.rooster"), Rooster.avroCodec)

  val ate = OptionalKV.ate(roosterTopic)

  val data: RDD[NJProducerRecord[Int, Rooster]] =
    RoosterData.rdd.map(x => NJProducerRecord(Random.nextInt(), x.copy(a = Instant.now())))

  implicit val te: TypedEncoder[OptionalKV[Int, Int]] = shapeless.cachedImplicit

  test("console sink") {
    val rooster = roosterTopic.withTopicName("sstream.console.rooster").in(ctx)
    val ss = sparKafka
      .topic(rooster)
      .sstream
      .checkpoint("./data/test/checkpoint")
      .map(x => x.newValue(x.value.map(_.index + 1)))
      .flatMap(x => x.value.map(_ => x))
      .filter(_ => true)
      .failOnDataLoss
      .ignoreDataLoss
      .progressInterval(3000)
      .consoleSink
      .trigger(Trigger.ProcessingTime(1000))
      .rows(3)
      .truncate
      .untruncate
      .append
      .complete
      .update
      .showProgress

    val upload =
      sparKafka.topic(rooster).prRdd(data).batchSize(1).triggerEvery(0.5.seconds).upload.delayBy(2.second)

    ss.concurrently(upload).interruptAfter(10.seconds).compile.drain.unsafeRunSync()
  }

  test("file sink avro - should be read back") {
    val rooster = roosterTopic.withTopicName("sstream.file.rooster").in(ctx)

    val path = root + "fileSink"
    val ss = sparKafka
      .topic(rooster)
      .sstream
      .ignoreDataLoss
      .fileSink(path)
      .triggerEvery(500.millisecond)
      .avro
      .withOptions(identity)
      .queryStream

    val upload = sparKafka
      .topic(rooster)
      .prRdd(data)
      .batchSize(10)
      .triggerEvery(0.1.second)
      .timeLimit(1000)
      .timeLimit(2.minute)
      .recordsLimit(10)
      .upload
      .delayBy(3.second)

    (ss.concurrently(upload).interruptAfter(6.seconds).compile.drain >>
      sparKafka.topic(rooster).load.avro(path).count.map(println)).unsafeRunSync()
  }

  test("date partition sink json - should be read back") {
    val rooster = roosterTopic.withTopicName("sstream.datepartition.rooster").in(ctx)

    val path = root + "date_partition"

    val ss = sparKafka
      .topic(rooster)
      .sstream
      .progressInterval(1000)
      .failOnDataLoss
      .datePartitionSink(path)
      .triggerEvery(1.seconds)
      .parquet
      .avro
      .json // last one wins
      .showProgress

    val upload =
      sparKafka
        .topic(rooster)
        .prRdd(data)
        .replicate(5)
        .batchSize(1)
        .triggerEvery(0.5.seconds)
        .upload
        .delayBy(1.second)
        .debug()
    ss.concurrently(upload).interruptAfter(6.seconds).compile.drain.unsafeRunSync()
    val ts        = NJTimestamp(Instant.now()).`Year=yyyy/Month=mm/Day=dd`(sydneyTime)
    val todayPath = path + "/" + ts
    assert(!File(todayPath).isEmpty, s"$todayPath does not exist")
    sparKafka.topic(rooster).load.json(todayPath).count.map(println).unsafeRunSync()
  }

  test("memory sink - validate kafka timestamp") {
    val rooster = roosterTopic.withTopicName("sstream.memory.rooster").in(ctx)

    val ss = sparKafka
      .topic(rooster)
      .sstream
      .ignoreDataLoss
      .memorySink("kafka")
      .trigger(Trigger.ProcessingTime(1000))
      .complete
      .append
      .queryStream

    val upload =
      sparKafka.topic(rooster).prRdd(data).batchSize(6).triggerEvery(1.second).upload.delayBy(3.second)
    ss.concurrently(upload).interruptAfter(6.seconds).compile.drain.unsafeRunSync()
    import sparkSession.implicits._
    val now = Instant.now().getEpochSecond * 1000 //to millisecond
    val size = sparkSession
      .sql("select timestamp from kafka")
      .as[Long]
      .collect()
      .map(t => assert(Math.abs(now - t) < 5000))
      .length
    assert(size == 4)
  }
}
