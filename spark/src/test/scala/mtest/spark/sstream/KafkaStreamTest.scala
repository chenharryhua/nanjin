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
    val ss = rooster.sparKafka.sstream.sstream
      .withParamUpdate(_.withProcessingTimeTrigger(500).withJson)
      .consoleSink
      .queryStream
      .interruptAfter(5.seconds)
    val upload = rooster.sparKafka.prRdd(data).upload.delayBy(1.second)

    ss.concurrently(upload).compile.drain.unsafeRunSync()
  }

  test("file sink") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("sstream.file.rooster"), Rooster.avroCodec).in(ctx)
    val ss = rooster.sparKafka.sstream.sstream
      .withParamUpdate(_.withProcessingTimeTrigger(500).withAvro)
      .fileSink(root + "fileSink")
      .queryStream
      .interruptAfter(5.seconds)
    val upload = rooster.sparKafka.prRdd(data).upload.delayBy(1.second)
    ss.concurrently(upload).compile.drain.unsafeRunSync()
  }

  test("date partition sink") {
    val rooster =
      TopicDef[Int, Rooster](TopicName("sstream.datepatition.rooster"), Rooster.avroCodec).in(ctx)
    val ss = rooster.sparKafka.sstream
      .withParamUpdate(_.withProcessingTimeTrigger(500).withParquet)
      .datePartitionFileSink(root + "datePartition")
      .queryStream
      .interruptAfter(5.seconds)
    val upload = rooster.sparKafka.prRdd(data).upload.delayBy(1.second)
    ss.concurrently(upload).compile.drain.unsafeRunSync()
  }
}
