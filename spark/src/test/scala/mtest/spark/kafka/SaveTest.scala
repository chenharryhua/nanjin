package mtest.spark.kafka

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat.{Avro, Jackson, Json, Parquet}
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.kafka._
import frameless.cats.implicits._
import fs2.kafka.ProducerRecord
import org.scalatest.funsuite.AnyFunSuite
import io.circe.generic.auto._

import scala.util.Random

object SaveTestData {
  final case class Foo(a: Int, b: String)

  val topic: KafkaTopic[IO, Int, Foo] =
    TopicDef[Int, Foo](TopicName("test.spark.kafka.save.load")).in(ctx)

}

class SaveTest extends AnyFunSuite {
  import SaveTestData._

  val list: List[ProducerRecord[Int, Foo]] =
    List.fill(100)(topic.fs2PR(Random.nextInt(), Foo(Random.nextInt(), "aaa")))
  val vlist: List[Foo] = list.map(_.value)

  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
    topic.send(list)).unsafeRunSync()

  test("dump") {
    topic.sparKafka.dump.unsafeRunSync()
  }
  test("jackson") {
    val action =
      topic.sparKafka.fromKafka.flatMap(_.saveJackson(blocker)).map(r => assert(r == 100)) >>
        sparkSession
          .jackson[NJConsumerRecord[Int, Foo]](
            topic.sparKafka.params.pathBuilder(topic.topicName, Jackson))
          .typedDataset
          .collect[IO]()
          .map(r => assert(r.sorted.flatMap(_.value).toList == vlist))
    action.unsafeRunSync()
  }

  test("json") {
    val action =
      topic.sparKafka.fromKafka.flatMap(_.saveJson(blocker)).map(r => assert(r == 100)) >>
        sparkSession
          .json[NJConsumerRecord[Int, Foo]](
            topic.sparKafka.params.pathBuilder(topic.topicName, Json))
          .typedDataset
          .collect[IO]()
          .map(r => assert(r.sorted.flatMap(_.value).toList == vlist))
    action.unsafeRunSync()
  }

  test("avro") {

    val action =
      topic.sparKafka.fromKafka.flatMap(_.saveAvro(blocker)).map(r => assert(r == 100)) >>
        sparkSession
          .avro[NJConsumerRecord[Int, Foo]](
            topic.sparKafka.params.pathBuilder(topic.topicName, Avro))
          .collect[IO]()
          .map(r => assert(r.sorted.flatMap(_.value).toList == vlist))
    action.unsafeRunSync()
  }
  test("parquet") {
    val action =
      topic.sparKafka.fromKafka.flatMap(_.saveParquet(blocker)).map(r => assert(r == 100)) >>
        sparkSession
          .parquet[NJConsumerRecord[Int, Foo]](
            topic.sparKafka.params.pathBuilder(topic.topicName, Parquet))
          .collect[IO]()
          .map(r => assert(r.sorted.flatMap(_.value).toList == vlist))
    action.unsafeRunSync()
  }
}
