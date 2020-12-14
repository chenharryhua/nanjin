package mtest.spark.sstream

import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark.kafka.{NJProducerRecord, _}
import mtest.spark.persist.{Ant, AntData, Rooster, RoosterData}
import mtest.spark.{contextShift, ctx, sparkSession, timer}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._
import scala.util.Random

class KafkaStreamTest extends AnyFunSuite {

  val root = "./data/test/spark/sstream/"

  test("ant") {
    val ant  = TopicDef[Int, Ant](TopicName("sstream.ant"), Ant.avroCodec).in(ctx)
    val data = AntData.rdd.map(x => NJProducerRecord(Random.nextInt(), x))

    val ss = ant.sparKafka.sstream.sstream
      .withParamUpdate(_.withProcessingTimeTrigger(1000).withAvro)
      .fileSink(root + "ant")
      .queryStream
      .interruptAfter(3.seconds)
    val upload = ant.sparKafka.prRdd(data).upload.delayBy(1.second)
    ss.concurrently(upload).compile.drain.unsafeRunSync()
  }

  test("console sink") {
    val rooster = TopicDef[Int, Rooster](TopicName("sstream.rooster"), Rooster.avroCodec).in(ctx)
    val data    = RoosterData.rdd.map(x => NJProducerRecord(Random.nextInt(), x))
    val ss = rooster.sparKafka.sstream.sstream
      .withParamUpdate(_.withProcessingTimeTrigger(1000).withJson)
      .consoleSink
      .queryStream
      .interruptAfter(3.seconds)
    val upload = rooster.sparKafka.prRdd(data).upload.delayBy(1.second)

    ss.concurrently(upload).compile.drain.unsafeRunSync()
  }

  test("date partition") {
    val rooster = TopicDef[Int, Rooster](TopicName("sstream.rooster"), Rooster.avroCodec).in(ctx)
    val data    = RoosterData.rdd.map(x => NJProducerRecord(Random.nextInt(), x))

    val ss = rooster.sparKafka.sstream
      .withParamUpdate(_.withProcessingTimeTrigger(1000).withParquet)
      .datePartitionFileSink(root + "date")
      .queryStream
      .interruptAfter(3.seconds)
    val upload = rooster.sparKafka.prRdd(data).upload.delayBy(1.second)
    ss.concurrently(upload).compile.drain.unsafeRunSync()
  }
}
