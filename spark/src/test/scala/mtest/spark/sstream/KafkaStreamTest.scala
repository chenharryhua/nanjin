package mtest.spark.sstream

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark.SparkSettings
import com.github.chenharryhua.nanjin.spark.kafka.{NJProducerRecord, _}
import mtest.spark.persist.{Ant, AntData}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._
import scala.util.Random

class KafkaStreamTest extends AnyFunSuite {

  test("ant") {
    val ant  = TopicDef[Int, Ant](TopicName("sstream.ant"), Ant.avroCodec).in(ctx)
    val data = AntData.rdd.map(x => NJProducerRecord(Random.nextInt(), x))

    val run = ant.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.flatMap { _ =>
      SparkSettings.default
        .sessionStream[IO]
        .flatMap { implicit ss =>
          val ss = ant.sparKafka.sstream.sstream
            .withParamUpdate(_.withProcessingTimeTrigger(1000))
            .fileSink("./data/test/spark/sstream/ant")
            .queryStream
            .interruptAfter(5.seconds)
          val upload = ant.sparKafka.prRdd(data).upload.delayBy(2.second)
          ss.concurrently(upload)
        }
        .compile
        .drain
    }
    run.unsafeRunSync()
  }
}
