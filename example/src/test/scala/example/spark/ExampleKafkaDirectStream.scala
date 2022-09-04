package example.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.dstream.DStreamRunner
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import example.*
import example.topics.fooTopic
import io.circe.generic.auto.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

@DoNotDiscover
class ExampleKafkaDirectStream extends AnyFunSuite {
  test("persist messages using direct streaming") {
    val path = NJPath("./data/example/foo/dstream")
    val cp   = NJPath("./data/example/foo/checkpoint")
    better.files.File(path.pathStr).delete(swallowIOExceptions = true)
    val runner = DStreamRunner[IO](sparKafka.sparkSession.sparkContext, cp, 2.seconds)
    runner
      .signup(sparKafka.topic(fooTopic).dstream)(_.coalesce.circe(path))
      .stream
      .interruptAfter(5.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }
}
