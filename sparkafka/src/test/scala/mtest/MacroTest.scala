package mtest

import java.time.LocalDateTime

import com.github.chenharryhua.nanjin.sparkafka.Sparkafka
import org.scalatest.FunSuite

class MacroTest extends FunSuite {
  test("sparkafka generate dataset") {
    import spark.implicits._
    val end   = LocalDateTime.now()
    val start = end.minusHours(1)
    Sparkafka.kafkaDS(spark, topic, start, end).map(_.map(_._2).count).map(println).unsafeRunSync()
    println(sparkSettings.show)
  }
}
