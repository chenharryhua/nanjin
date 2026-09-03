package mtest.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.config.{
  KafkaConsumerSettings,
  KafkaProducerSettings,
  KafkaSettings,
  KafkaStreamSettings,
  SerdeSettings
}
import com.github.chenharryhua.nanjin.kafka.KafkaContext
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

  test("1.consumeBytes does not require schema registry configuration") {
    noException shouldBe thrownBy(ctx.consumeBytes("raw-bytes"))
  }
}
