package example.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.dstream.DStreamRunner
import example.*
import example.topics.fooTopic
import io.circe.generic.auto.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

@DoNotDiscover
class ExampleKafkaDirectStream extends AnyFunSuite {
  test("persist messages using direct streaming") {
    val path = "./data/example/foo/dstream"
    better.files.File(path).delete(true)
    val runner = DStreamRunner[IO](sparKafka.sparkSession.sparkContext, "./data/example/foo/checkpoint", 2.seconds)
    runner.signup(sparKafka.topic(fooTopic).dstream)(_.coalesce.circe(path)).stream.compile.drain.unsafeRunSync()
  }
}
