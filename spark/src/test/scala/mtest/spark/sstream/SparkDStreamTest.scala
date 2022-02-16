package mtest.spark.sstream

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.{NJLogLevel, PathSegment}
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJTimestamp}
import com.github.chenharryhua.nanjin.spark.dstream.DStreamRunner
import com.github.chenharryhua.nanjin.spark.kafka.SparKafkaTopic
import com.github.chenharryhua.nanjin.terminals.NJPath
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerRecords}
import io.circe.generic.auto.*
import mtest.spark.kafka.sparKafka
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter, DoNotDiscover}

import scala.concurrent.duration.*
import eu.timepit.refined.auto.*

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
    .through(topic.topic.fs2Channel.updateProducer(_.withClientId("spark.kafka.dstream.test")).producerPipe)
    .interruptAfter(10.seconds)

  test("dstream") {
    val jackson    = root / "jackson"
    val circe      = root / "circe"
    val avro       = root / "avro"
    val checkpoint = root / "checkpont"

    val runner: DStreamRunner[IO] = DStreamRunner[IO](sparKafka.sparkSession.sparkContext, checkpoint, 3.second)
    sender
      .concurrently(
        runner.withFreshStart
          .signup(topic.dstream)(_.avro(avro).snappy.run)
          .signup(topic.dstream)(_.coalesce.jackson(jackson).deflate(2).run)
          .signup(topic.dstream)(_.coalesce.circe(circe).gzip.run)
          .stream
          .debug()
      )
      .compile
      .drain
      .unsafeRunSync()

    val ts    = NJTimestamp.now()
    val year  = PathSegment.unsafeFrom(s"Year=${ts.yearStr(sydneyTime)}")
    val month = PathSegment.unsafeFrom(s"Month=${ts.monthStr(sydneyTime)}")
    val day   = PathSegment.unsafeFrom(s"Day=${ts.dayStr(sydneyTime)}")

    val j = topic.load.jackson(jackson / year / month / day).unsafeRunSync().transform(_.distinct())
    val a = topic.load.avro(avro / year / month / day).map(_.transform(_.distinct())).unsafeRunSync()
    val c = topic.load.circe(circe / year / month / day).unsafeRunSync().transform(_.distinct())

    j.diff(a).show(truncate = false)
    c.diff(a).show(truncate = false)
    j.diff(c).show(truncate = false)
    (j.count, a.count, c.count).mapN((a, b, c) => println((a, b, c))).unsafeRunSync()
  }
}
