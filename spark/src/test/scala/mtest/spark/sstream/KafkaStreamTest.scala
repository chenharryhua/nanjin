package mtest.spark.sstream

import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark.kafka._
import com.github.chenharryhua.nanjin.spark.kafka.{NJProducerRecord, OptionalKV}
import mtest.spark.persist.{Ant, AntData}
import org.scalatest.funsuite.AnyFunSuite
import scala.concurrent.duration._
import scala.util.Random

class KafkaStreamTest extends AnyFunSuite {

  test("ant") {
    val ant  = TopicDef[Int, Ant](TopicName("sstream.ant"), Ant.avroCodec).in(ctx)
    val data = AntData.rdd.map(x => NJProducerRecord(Random.nextInt(), x))

    val ss = ant.sparKafka.sstream.sstream
      .withParamUpdate(_.withProcessingTimeTrigger(1000))
      .fileSink("./data/test/spark/sstream/ant")
      .queryStream
      .interruptAfter(5.seconds)
    val upload = ant.sparKafka.prRdd(data).upload.delayBy(2.second)

    val run = for {
      _ <- ant.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- ss.concurrently(upload).compile.drain
    } yield ()

    run.unsafeRunSync()

  }

}
