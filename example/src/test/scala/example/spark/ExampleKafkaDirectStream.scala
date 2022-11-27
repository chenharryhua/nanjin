package example.spark

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.dstream.DStreamRunner
import com.github.chenharryhua.nanjin.spark.listeners.SparkDStreamListener
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import example.*
import example.topics.fooTopic
import io.circe.generic.auto.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import fs2.Stream
@DoNotDiscover
class ExampleKafkaDirectStream extends AnyFunSuite {
  test("persist messages using direct streaming") {
    val path = NJPath("./data/example/foo/dstream")
    val cp   = NJPath("./data/example/foo/checkpoint")
    better.files.File(path.pathStr).delete(swallowIOExceptions = true)
    for {
      ds <- Stream.resource(
        DStreamRunner[IO](sparKafka.sparkSession.sparkContext, cp, 2.seconds)
          .signup(sparKafka.topic(fooTopic).dstream)(_.coalesce.circe(path))
          .resource)
      evt <- SparkDStreamListener[IO](ds)
    } yield evt
  }
}
