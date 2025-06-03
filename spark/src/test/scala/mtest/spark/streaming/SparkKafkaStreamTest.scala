package mtest.spark.streaming

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.spark.SchematizedEncoder
import com.github.chenharryhua.nanjin.spark.persist.{Rooster, RoosterData}
import eu.timepit.refined.auto.*
import frameless.TypedEncoder
import mtest.spark.kafka.{ctx, sparKafka}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.concurrent.duration.DurationInt
import scala.util.Random

@DoNotDiscover
class SparkKafkaStreamTest extends AnyFunSuite {

  val root = "./data/test/spark/sstream/"

  val roosterTopic: TopicDef[Int, Rooster] =
    TopicDef[Int, Rooster](TopicName("sstream.rooster"), Rooster.avroCodec)

  val ate = SchematizedEncoder(roosterTopic)

  val data: List[NJProducerRecord[Int, Rooster]] =
    RoosterData.data.map(x =>
      NJProducerRecord(roosterTopic.topicName, Random.nextInt(), x.copy(a = Instant.now())))

  implicit val te: TypedEncoder[NJConsumerRecord[Int, Int]] = shapeless.cachedImplicit

//  test("console sink") {
//    val rooster = roosterTopic.withTopicName("sstream.console.rooster").in(ctx)
//    val ss = sparKafka
//      .topic(rooster)
//      .sstream
//      .checkpoint(NJPath("./data/test/checkpoint"))
//      .map(x => x.newValue(x.value.map(_.index + 1)))
//      .flatMap(x => x.value.map(_ => x))
//      .filter(_ => true)
//      .failOnDataLoss
//      .ignoreDataLoss
//      .progressInterval(3000)
//      .consoleSink
//      .trigger(Trigger.ProcessingTime(1000))
//      .rows(3)
//      .truncate
//      .untruncate
//      .append
//      .complete
//      .update
//      .showProgress
//
//    val upload =
//      sparKafka
//        .topic(rooster)
//        .prRdd(data)
//        .producerRecords(rooster.topicName, 1)
//        .through(rooster.produce.pipe)
//        .metered(0.2.seconds)
//        .delayBy(2.second)
//
//    ss.concurrently(upload).interruptAfter(10.seconds).compile.drain.unsafeRunSync()
//  }
//
  test("console") {
    import sparKafka.sparkSession.implicits.*
    val rooster = ctx.topic(roosterTopic.withTopicName("sstream.file.rooster"))

    sparKafka
      .sstream(rooster.topicName)
      .flatMap(rooster.serde.tryDeserialize(_).toOption)
      .writeStream
      .format("console")
      .start()

    val upload = sparKafka
      .topic(rooster.topicDef)
      .prRdd(data)
      .withTopicName(rooster.topicName)
      .replicate(100)
      .producerRecords[IO](1)
      .metered(1.seconds)
      .through(ctx.produce(rooster.topicDef.rawSerdes).sink)

    upload.interruptAfter(10.seconds).compile.drain.unsafeRunSync()
  }
//
//  test("memory sink - validate kafka timestamp") {
//    val rooster = roosterTopic.withTopicName("sstream.memory.rooster").in(ctx)
//
//    val ss = sparKafka
//      .topic(rooster)
//      .sstream
//      .ignoreDataLoss
//      .memorySink("kafka")
//      .trigger(Trigger.ProcessingTime(1000))
//      .complete
//      .append
//      .stream
//
//    val upload =
//      sparKafka
//        .topic(rooster)
//        .prRdd(data)
//        .producerRecords(rooster.topicName, 1)
//        .metered(1.second)
//        .through(rooster.produce.pipe)
//        .delayBy(3.second)
//    ss.concurrently(upload).interruptAfter(6.seconds).compile.drain.unsafeRunSync()
//    import sparkSession.implicits.*
//    val now = Instant.now().getEpochSecond * 1000 // to millisecond
//    val size = sparkSession
//      .sql("select timestamp from kafka")
//      .as[Long]
//      .collect()
//      .map(t => assert(Math.abs(now - t) < 10000))
//      .length
//    assert(size == 4)
//  }
}
