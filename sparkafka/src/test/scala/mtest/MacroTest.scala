package mtest

import java.time.LocalDateTime

import com.github.chenharryhua.nanjin.sparkafka.Sparkafka
import org.scalatest.FunSuite

class MacroTest extends FunSuite {
  test("sparkafka should be able to create dataset") {
    import spark.implicits._
    val end   = LocalDateTime.now()
    val start = end.minusHours(1)
    Sparkafka
      .kafkaDS(spark, topics.payment.in(ctx), start, end)
      .map(_.map(_._2).show)
      .unsafeRunSync()
  }
}
