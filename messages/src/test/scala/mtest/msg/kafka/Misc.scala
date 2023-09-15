package mtest.msg.kafka

import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import fs2.kafka.ConsumerRecord
import org.scalatest.funsuite.AnyFunSuite
import io.scalaland.chimney.dsl.*
import cats.syntax.all.*

class Misc extends AnyFunSuite {
  test("consumer record conversion") {
    val cr = NJConsumerRecord[Int, Int](
      partition = 0,
      offset = 0,
      timestamp = 0,
      key = 1.some,
      value = "a".some,
      topic = "topic",
      timestampType = 0,
      headers = Nil)

    cr.transformInto[ConsumerRecord[Option[Int], Option[Int]]]

  }

}
