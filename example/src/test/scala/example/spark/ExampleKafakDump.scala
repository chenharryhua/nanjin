package example.spark

import cats.effect.unsafe.implicits.global
import example.sparKafka
import example.topics.fooTopic
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import fs2.Stream

@DoNotDiscover
class ExampleKafakDump extends AnyFunSuite {
  test("dump kafka data in json") {
    val path = "./data/example/foo/batch/circe.json"
    Stream.eval(sparKafka.topic(fooTopic).fromKafka).flatMap(_.save.circe(path).file.sink).compile.drain.unsafeRunSync()
  }
  test("dump kafka data in avro compressed by snappy") {
    val path = "./data/example/foo/batch/avro"
    sparKafka.topic(fooTopic).fromKafka.flatMap(_.save.avro(path).snappy.folder.run).unsafeRunSync()
  }
}
