package mtest.spark.sstream

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.NJLogLevel
import com.github.chenharryhua.nanjin.spark.dstream.DStreamRunner
import com.github.chenharryhua.nanjin.spark.kafka.SparKafkaTopic
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerRecords}
import io.circe.generic.auto.*
import mtest.spark.kafka.sparKafka
import org.scalatest.{BeforeAndAfter, DoNotDiscover}
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDate
import scala.concurrent.duration.*

@DoNotDiscover
class SparkDStreamTest extends AnyFunSuite with BeforeAndAfter {

  before(sparKafka.sparkSession.sparkContext.setLogLevel(NJLogLevel.ERROR.entryName))
  after(sparKafka.sparkSession.sparkContext.setLogLevel(NJLogLevel.WARN.entryName))

  val root: NJPath = NJPath("./data/test/spark/dstream/")

//  better.files.File(root).delete(true)

  val topic: SparKafkaTopic[IO, Int, String] = sparKafka.topic[Int, String]("dstream.test")

  val sender = Stream
    .awakeEvery[IO](0.3.seconds)
    .zipWithIndex
    .map { case (_, idx) =>
      ProducerRecords.one(ProducerRecord(topic.topicName.value, idx.toInt, "a"))
    }
    .debug()
    .through(topic.topic.produce.updateConfig(_.withClientId("spark.kafka.dstream.test")).pipe)
    .interruptAfter(10.seconds)

  test("dstream") {
    val jackson    = root / "jackson"
    val circe      = root / "circe"
    val avro       = root / "avro"
    val checkpoint = root / "checkpont"

    val runner: DStreamRunner[IO] =
      DStreamRunner[IO](sparKafka.sparkSession.sparkContext, checkpoint, 3.second)
    sender
      .concurrently(
        runner.withFreshStart
          .signup(topic.dstream)(_.coalesce.avro(avro))
          .signup(topic.dstream)(_.coalesce.jackson(jackson))
          .signup(topic.dstream)(_.coalesce.circe(circe))
          .stream
          .debug()
      )
      .compile
      .drain
      .unsafeRunSync()

    val ts = LocalDate.now()

    val j = topic.load.jackson(jackson / ts).transform(_.distinct())
    val a = topic.load.avro(avro / ts).transform(_.distinct())
    val c = topic.load.circe(circe / ts).transform(_.distinct())

    (j.count, a.count, c.count).mapN((a, b, c) => println((a, b, c))).unsafeRunSync()
  }
}
