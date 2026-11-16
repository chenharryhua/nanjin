package mtest.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.Event.{MetricsSnapshot, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.observers.kafka.KafkaObserver
import com.github.chenharryhua.nanjin.kafka.KafkaContext
import com.github.chenharryhua.nanjin.kafka.config.KafkaSettings
import munit.CatsEffectSuite

class KafkaObserverTest extends CatsEffectSuite {
  private val topic = "observer"
  private val ctx = KafkaContext[IO](KafkaSettings.local)
  test("1.observer") {
    TaskGuard[IO]("observer")
      .service("observer")
      .eventStream(_ => IO(()))
      .through(KafkaObserver(ctx).withTranslator(_.skipMetricsSnapshot).observe(topic))
      .compile
      .drain
  }
}

class KafkaObserverTranslatorTest extends CatsEffectSuite {
  test("skipped translations leave the event stream unchanged") {
    val ctx = KafkaContext[IO](KafkaSettings.local)
    val source = TaskGuard[IO]("observer")
      .service("observer")
      .eventStream(_.adhoc.report)

    for {
      input <- source.compile.toList
      events <- fs2.Stream.emits(input).covary[IO]
        .through(KafkaObserver(ctx).withTranslator(_.skipAll).observe("observer"))
        .compile
        .toList
    } yield {
      assert(input.exists(_.isInstanceOf[ServiceStart]))
      assert(input.exists(_.isInstanceOf[MetricsSnapshot]))
      assert(input.exists(_.isInstanceOf[ServiceStop]))
      assertEquals(events, input)
    }
  }
}
