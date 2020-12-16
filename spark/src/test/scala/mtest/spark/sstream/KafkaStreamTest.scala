package mtest.spark.sstream

import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark.kafka.{NJProducerRecord, _}
import mtest.spark.persist.{Rooster, RoosterData}
import mtest.spark.{contextShift, ctx, sparkSession, timer}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._
import scala.util.Random

class KafkaStreamTest extends AnyFunSuite {

  val root = "./data/test/spark/sstream/"

  val data = RoosterData.rdd.map(x => NJProducerRecord(Random.nextInt(), x))

  test("console sink") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("sstream.console.rooster"), Rooster.avroCodec).in(ctx)
    val ss = rooster.sparKafka.sstream
      .map(x => x.newValue(x.value.map(_.index + 1)))
      .flatMap(x => x.value.map(_ => x))
      .sstream
      .withParamUpdate(
        _.withProcessingTimeTrigger(
          500).withJson.withUpdate.withComplete.withAppend.failOnDataLoss.ignoreDataLoss
          .withCheckpointReplace("./data/test/sstream/checkpoint/")
          .withProgressInterval(1000)
          .withProgressInterval(1.seconds))
      .map(List(_))
      .filter(_ => true)
      .flatMap(identity)
      .transform(identity)
      .consoleSink
      .showProgress
      .interruptAfter(6.seconds)
    val upload = rooster.sparKafka.prRdd(data).batch(1).interval(1000).upload.delayBy(1.second)

    ss.concurrently(upload).compile.drain.unsafeRunSync()
  }

  test("file sink") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("sstream.file.rooster"), Rooster.avroCodec).in(ctx)
    val ss = rooster.sparKafka.sstream.sstream
      .withParamUpdate(_.withProcessingTimeTrigger(500).withAvro.withContinousTrigger(1000))
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
    val ss = rooster.sparKafka.sstream
      .withParamUpdate(_.withProcessingTimeTrigger(500).withParquet)
      .datePartitionFileSink(root + "datePartition")
      .queryStream
      .interruptAfter(10.seconds)
    val upload = rooster.sparKafka.prRdd(data).upload.delayBy(1.second)
    ss.concurrently(upload).compile.drain.unsafeRunSync()
  }
}
