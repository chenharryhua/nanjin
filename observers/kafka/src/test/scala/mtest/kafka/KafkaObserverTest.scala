package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.KafkaObserver
import com.github.chenharryhua.nanjin.kafka.KafkaSettings
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*

class KafkaObserverTest extends AnyFunSuite {
  val topic = TopicName("observer")
  val ctx   = KafkaSettings.local.ioContext
  test("observer") {
    TaskGuard[IO]("observer")
      .service("observer")
      .eventStream(_.action("observer", _.bipartite).retry(IO(())).run)
      .through(KafkaObserver(ctx).updateTranslator(_.skipMetricReport).observe(topic))
      .debug()
      .compile
      .drain
      .unsafeRunSync()
  }
}
