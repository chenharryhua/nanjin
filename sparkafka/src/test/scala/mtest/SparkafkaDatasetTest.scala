package mtest

import java.time.LocalDateTime

import cats.effect.IO
import com.github.chenharryhua.nanjin.sparkafka.Sparkafka
import org.scalatest.FunSuite
import frameless.cats.implicits._
import cats.implicits._
import cats.derived.auto.show._

class SparkafkaDatasetTest extends FunSuite {
  test("sparkafka should be able to create dataset") {

    val end   = LocalDateTime.now()
    val start = end.minusHours(1)
    spark.use { s =>
      import s.implicits._
      Sparkafka.valueDataset(s, payment, start, end).flatMap(_.take[IO](10))
    }.map(_.map(_.show)).map(x => x.foreach(println)).unsafeRunSync
  }
}
