package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.TopicName
import org.scalatest.funsuite.AnyFunSuite

class TopicSyntaxTest extends AnyFunSuite {
  test("topic name") {
    val tn: TopicName = "abc.unsafe"
    val tn2 = TopicName("abc.checked")
    println((tn, tn2))
  }
}
