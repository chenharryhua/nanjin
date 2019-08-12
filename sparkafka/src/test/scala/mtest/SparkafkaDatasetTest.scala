package mtest

import java.time.LocalDateTime

import com.github.chenharryhua.nanjin.sparkafka.Sparkafka
import org.scalatest.FunSuite

class SparkafkaDatasetTest extends FunSuite {
  test("sparkafka should be able to create dataset") {

    val end   = LocalDateTime.now()
    val start = end.minusHours(1)
    spark.use { s =>
      import s.implicits._
      Sparkafka.kafkaDS(s, payment, start, end).map(_.dataset.map(_._2).show)
    }.unsafeRunSync
  }
}
