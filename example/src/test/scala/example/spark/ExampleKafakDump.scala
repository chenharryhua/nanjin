package example.spark

import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import example.sparKafka
import example.topics.fooTopic
import fs2.Stream
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class ExampleKafakDump extends AnyFunSuite {
  test("dump kafka data in json") {
    val path = NJPath("./data/example/foo/batch/circe.json")
    sparKafka.topic(fooTopic).fromKafka.flatMap(_.save(path).circe.run).unsafeRunSync()
  }
  test("dump kafka data in avro compressed by snappy") {
    val path = NJPath("./data/example/foo/batch/avro")
    sparKafka.topic(fooTopic).fromKafka.flatMap(_.save(path).avro.snappy.run).unsafeRunSync()
  }
}
