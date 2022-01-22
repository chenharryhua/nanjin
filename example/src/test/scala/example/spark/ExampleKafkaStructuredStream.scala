package example.spark

import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import example.*
import example.topics.fooTopic
import io.circe.generic.auto.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

@DoNotDiscover
class ExampleKafkaStructuredStream extends AnyFunSuite {
  test("persist messages using structured streaming") {
    val path = NJPath("./data/example/foo/sstream")
    sparKafka
      .topic(fooTopic)
      .sstream
      .datePartitionSink(path)
      .parquet
      .stream
      .interruptAfter(3.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }
}
