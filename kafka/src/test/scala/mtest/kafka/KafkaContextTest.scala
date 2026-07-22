package mtest.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.{KafkaConsumerSettings, KafkaContext, KafkaProducerSettings, KafkaSettings, KafkaStreamSettings, SerdeSettings, TopicName}
import fs2.kafka.AdminClientSettings
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class KafkaContextTest extends AnyFunSuite with Matchers {
  private val ctx = KafkaContext[IO](
    KafkaSettings(
      KafkaConsumerSettings(Map.empty),
      KafkaProducerSettings(Map.empty),
      AdminClientSettings("broker-url"),
      KafkaStreamSettings(Map.empty),
      SerdeSettings(Map.empty)
    )
  )

  test("consumeBytes does not require schema registry configuration") {
    noException shouldBe thrownBy(ctx.consumeBytes(TopicName("raw-bytes")))
  }
}