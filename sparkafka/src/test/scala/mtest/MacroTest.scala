package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.sparkafka.{MyDecoder, SparkafkaMacro}
import org.scalatest.FunSuite

class MacroTest extends FunSuite {
  val topic = ctx.topic[Int, Int]("abc")
  test("m") {

    val dec: MyDecoder[Int, Int] = SparkafkaMacro.decoder[IO, Int, Int](topic)
    val data                     = topic.keyIso.reverseGet(100)
    println(dec.key(data))
  }
}
