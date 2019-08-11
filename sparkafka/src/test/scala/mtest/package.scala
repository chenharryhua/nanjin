import cats.effect.{ContextShift, IO, Timer}
import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaSettings}

import scala.concurrent.ExecutionContext.Implicits.global

package object mtest {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO]     = IO.timer(global)

  val ctx: IoKafkaContext = KafkaSettings.local.ioContext
}
