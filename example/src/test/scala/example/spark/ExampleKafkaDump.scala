package example.spark

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import example.sparKafka
import example.topics.fooTopic
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import io.circe.generic.auto.*

@DoNotDiscover
class ExampleKafkaDump extends AnyFunSuite {
  test("dump kafka data in json") {
    val path = NJPath("./data/example/foo/batch/circe.json")
    sparKafka.topic(fooTopic).fromKafka.flatMap(_.output.circe(path).run[IO]).unsafeRunSync()
  }
  test("dump kafka data in avro compressed by snappy") {
    val path = NJPath("./data/example/foo/batch/avro")
    sparKafka
      .topic(fooTopic)
      .fromKafka
      .flatMap(_.output.avro(path).withCompression(_.Snappy).run[IO])
      .unsafeRunSync()
  }
}
