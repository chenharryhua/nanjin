package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.kafka.KafkaObserver
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaSettings}
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*

class KafkaObserverTest extends AnyFunSuite {
  private val topic = TopicName("observer")
  private val ctx = KafkaContext[IO](KafkaSettings.local)
  test("observer") {
    TaskGuard[IO]("observer")
      .service("observer")
      .eventStream(_ => IO(()))
      .through(KafkaObserver(ctx).updateTranslator(_.skipMetricReport).observe(topic))
      .debug()
      .compile
      .drain
      .unsafeRunSync()
  }
}
