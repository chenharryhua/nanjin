package mtest.spark.kafka

import java.time.Instant

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.datetime._
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import com.sksamuel.avro4s.ScalePrecision
import frameless.cats.implicits._
import org.scalatest.funsuite.AnyFunSuite

import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode

object DecimalTopicTestCase {
  final case class HasDecimal(a: BigDecimal, b: Instant)

  val data: HasDecimal =
    HasDecimal(BigDecimal(1.0101010101), Instant.ofEpochMilli(Instant.now.toEpochMilli))
}

class DecimalTopicTest extends AnyFunSuite {
  import DecimalTopicTestCase._
  implicit val sp: ScalePrecision                          = ScalePrecision(10, 20)
  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP
  val topic: KafkaTopic[IO, Int, HasDecimal]               = ctx.topic[Int, HasDecimal]("decimal.test")
  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
    topic.schemaRegister >>
    topic.send(1, data) >> topic.send(2, data)).unsafeRunSync()

  test("sparKafka kafka and spark agree on avro") {
    topic.fs2Channel.stream
      .map(m => topic.njDecoder.decode(m).run._2)
      .take(2)
      .through(fileSink(blocker).avro("./data/test/spark/kafka/decimal.avro"))
      .compile
      .drain
      .unsafeRunSync

    val res: List[HasDecimal] =
      sparkSession
        .avro[OptionalKV[Int, HasDecimal]]("./data/test/spark/kafka/decimal.avro")
        .collect[IO]
        .unsafeRunSync
        .toList
        .flatMap(_.value)

    assert(res === List(data, data))
  }

}
