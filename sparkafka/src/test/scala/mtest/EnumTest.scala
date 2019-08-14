package mtest

import java.time.LocalDateTime

import cats.effect.IO
import com.github.chenharryhua.nanjin.sparkafka.{Sparkafka, SparkafkaConsumerRecord}
import org.scalatest.FunSuite
import frameless.cats.implicits._
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import frameless.Injection
import fs2.Chunk

sealed trait Colorish

object Colorish {
  implicit val colorInjection: Injection[Colorish, String] = new Injection[Colorish, String] {
    override def apply(a: Colorish): String = a match {
      case Red   => "red"
      case Blue  => "blue"
      case Green => "green"
    }

    override def invert(b: String): Colorish = b match {
      case "red"   => Red
      case "blue"  => Blue
      case "green" => Green
    }
  }
  case object Red extends Colorish
  case object Green extends Colorish
  case object Blue extends Colorish
}

final case class Pencil(name: String, color: Colorish)

class EnumTest extends FunSuite {
  pencil_topic.schemaRegistry.register.unsafeRunSync()

  val pencils =
    List(
      (1, Pencil("steal", Colorish.Red)),
      (2, Pencil("wood", Colorish.Green)),
      (3, Pencil("plastic", Colorish.Blue)))
  pencil_topic.producer.send(pencils).unsafeRunSync()

  test("spark read") {

    val end   = LocalDateTime.now()
    val start = end.minusHours(1)
    fs2.Stream
      .eval(spark.use { s =>
        Sparkafka.kafkaDS(s, pencil_topic, start, end).flatMap(_.take[IO](10)).map(Chunk.seq)
      })
      .flatMap(fs2.Stream.chunk)
      .map(_.show)
      .showLinesStdOut
      .compile
      .drain
      .unsafeRunSync()

  }
}
