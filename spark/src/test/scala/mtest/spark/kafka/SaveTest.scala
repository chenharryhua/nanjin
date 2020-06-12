package mtest.spark.kafka

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark.kafka._
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object SaveTestData extends AnyFunSuite {
  final case class Foo(a: Int, b: String)
  val topic: KafkaTopic[IO, Int, Foo] = TopicDef[Int, Foo](TopicName("test.serDeser")).in(ctx)

}

class SaveTest extends AnyFunSuite {
  import SaveTestData._
  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
    topic.send(List.fill(100)(topic.fs2PR(Random.nextInt(), Foo(Random.nextInt(), "aaa")))))
    .unsafeRunSync()

  test("dump") {
    topic.sparKafka.dump.unsafeRunSync()
  }
  test("jackson") {
    assert(topic.sparKafka.fromKafka.flatMap(_.save).unsafeRunSync() == 100)
  }
  test("avro") {
    assert(
      topic.sparKafka.withParamUpdate(_.withAvro).fromKafka.flatMap(_.save).unsafeRunSync() == 100)
  }
  test("parquet") {
    assert(
      topic.sparKafka
        .withParamUpdate(_.withParquet)
        .fromKafka
        .flatMap(_.save)
        .unsafeRunSync() == 100)
  }
}
