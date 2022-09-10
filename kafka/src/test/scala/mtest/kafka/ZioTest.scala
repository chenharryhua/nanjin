package mtest.kafka

import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.scalatest.funsuite.AnyFunSuite
import zio.interop.catz.*
import zio.Task

import scala.concurrent.duration.*

class ZioTest extends AnyFunSuite {

  val runtime = zio.Runtime.default

  val ctx: KafkaContext[Task] =
    KafkaSettings.local.withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest").zioContext

  val topic: KafkaTopic[Task, Array[Byte], trip_record] =
    TopicDef[Array[Byte], trip_record](TopicName("nyc_yellow_taxi_trip_data")).in(ctx)

  test("zio should just work.") {
    val task = topic.consume.stream
      .map(m => topic.decoder(m).tryDecode)
      .map(_.toEither)
      .rethrow
      .take(1)
      .map(_.toString)
      .map(println)
      .interruptAfter(5.seconds)
      .compile
      .drain
    runtime.run(task.exitCode)
  }

}
