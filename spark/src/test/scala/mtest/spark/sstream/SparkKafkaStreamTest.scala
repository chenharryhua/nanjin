package mtest.spark.sstream

import better.files.*
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.PathRoot
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.spark.kafka.*
import com.github.chenharryhua.nanjin.terminals.NJPath
import frameless.TypedEncoder
import mtest.spark.kafka.{ctx, sparKafka}
import mtest.spark.persist.{Rooster, RoosterData}
import mtest.spark.sparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.concurrent.duration.*
import scala.util.Random
import eu.timepit.refined.auto.*

@DoNotDiscover
class SparkKafkaStreamTest extends AnyFunSuite {

  val root = NJPath("./data/test/spark/sstream/")

  val roosterTopic: TopicDef[Int, Rooster] =
    TopicDef[Int, Rooster](TopicName("sstream.rooster"), Rooster.avroCodec)

  val ate = NJConsumerRecord.ate(roosterTopic)

  val data: RDD[NJProducerRecord[Int, Rooster]] =
    RoosterData.rdd.map(x => NJProducerRecord(Random.nextInt(), x.copy(a = Instant.now())))

  implicit val te: TypedEncoder[NJConsumerRecord[Int, Int]] = shapeless.cachedImplicit

  test("console sink") {
    val rooster = roosterTopic.withTopicName("sstream.console.rooster").in(ctx)
    val ss = sparKafka
      .topic(rooster)
      .sstream
      .checkpoint(NJPath("./data/test/checkpoint"))
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
      sparKafka.topic(rooster).prRdd(data).withInterval(0.5.seconds).upload.stream.delayBy(2.second)

    ss.concurrently(upload).interruptAfter(10.seconds).compile.drain.unsafeRunSync()
  }

  test("file sink avro - should be read back") {
    val rooster = roosterTopic.withTopicName("sstream.file.rooster").in(ctx)

    val path = root / "fileSink"
    val ss = sparKafka
      .topic(rooster)
      .sstream
      .ignoreDataLoss
      .fileSink(path)
      .triggerEvery(500.millisecond)
      .json
      .parquet
      .avro
      .withOptions(identity)
      .stream

    val upload = sparKafka
      .topic(rooster)
      .prRdd(data)
      .withInterval(0.1.second)
      .withTimeLimit(2.minute)
      .withRecordsLimit(10)
      .upload
      .stream
      .delayBy(3.second)

    (ss.concurrently(upload).interruptAfter(6.seconds).compile.drain >>
      sparKafka.topic(rooster).load.avro(path).flatMap(_.count).map(println)).unsafeRunSync()
  }

  test("date partition sink json - should be read back") {
    val rooster = roosterTopic.withTopicName("sstream.datepartition.rooster").in(ctx)

    val path = root / "date_partition"

    val ss = sparKafka
      .topic(rooster)
      .jsonStream
      .progressInterval(1000)
      .failOnDataLoss
      .datePartitionSink(path)
      .triggerEvery(1.seconds)
      .json // last one wins
      .showProgress

    val upload =
      sparKafka
        .topic(rooster)
        .prRdd(data)
        .replicate(5)
        .withInterval(0.5.seconds)
        .upload
        .updateProducer(_.withClientId("spark.kafka.streaming.test"))
        .stream
        .delayBy(1.second)
        .debug()
    ss.concurrently(upload).interruptAfter(6.seconds).compile.drain.unsafeRunSync()
    val ts        = NJTimestamp(Instant.now()).`Year=yyyy/Month=mm/Day=dd`(sydneyTime)
    val todayPath = path.pathStr + "/" + ts
    assert(!File(todayPath).isEmpty, s"$todayPath does not exist")
    sparKafka
      .topic(rooster)
      .load
      .json(NJPath(PathRoot.unsafeFrom(todayPath)))
      .flatMap(_.count.map(println))
      .unsafeRunSync()
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
      .stream

    val upload =
      sparKafka.topic(rooster).prRdd(data).withInterval(1.second).upload.stream.delayBy(3.second)
    ss.concurrently(upload).interruptAfter(6.seconds).compile.drain.unsafeRunSync()
    import sparkSession.implicits.*
    val now = Instant.now().getEpochSecond * 1000 // to millisecond
    val size = sparkSession
      .sql("select timestamp from kafka")
      .as[Long]
      .collect()
      .map(t => assert(Math.abs(now - t) < 10000))
      .length
    assert(size == 4)
  }
}
