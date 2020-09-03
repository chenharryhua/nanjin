package mtest.spark.kafka

import java.time.Instant

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.persist.loaders
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
  implicit val codec                                       = NJAvroCodec[OptionalKV[Int, HasDecimal]]
  val topic: KafkaTopic[IO, Int, HasDecimal]               = ctx.topic[Int, HasDecimal]("decimal.test")
  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
    topic.schemaRegister >>
    topic.send(1, data) >> topic.send(2, data)).unsafeRunSync()

  test("sparKafka kafka and spark agree on avro") {
    val path = "./data/test/spark/kafka/decimal.avro"
    topic.fs2Channel.stream
      .map(m => topic.njDecoder.decode(m).run._2)
      .take(2)
      .through(fileSink(blocker).avro(path))
      .compile
      .drain
      .unsafeRunSync

    assert(loaders.raw.avro[OptionalKV[Int, HasDecimal]](path).collect().head.value.get == data)

  }
}
