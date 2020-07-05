package mtest.spark.kafka

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat.{Avro, Jackson, Json, Parquet}
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.kafka._
import frameless.cats.implicits._
import fs2.kafka.ProducerRecord
import org.scalatest.funsuite.AnyFunSuite
import io.circe.generic.auto._

import scala.util.Random

object SaveTestData {
  final case class Chicken(a: Int, b: String)

  val topic: KafkaTopic[IO, Int, Chicken] =
    TopicDef[Int, Chicken](TopicName("test.spark.kafka.save.load")).in(ctx)

}

class SaveTest extends AnyFunSuite {
  import SaveTestData._

  val chickenPR: List[ProducerRecord[Int, Chicken]] =
    List.fill(100)(topic.fs2PR(Random.nextInt(), Chicken(Random.nextInt(), "aaa")))
  val chickens: List[Chicken] = chickenPR.map(_.value)

  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
    topic.send(chickenPR)).unsafeRunSync()

  test("dump") {
    topic.sparKafka.dump.unsafeRunSync()
  }
  test("jackson") {
    val action =
      topic.sparKafka.fromKafka.flatMap(_.saveJackson(blocker)).map(r => assert(r == 100)) >>
        sparkSession
          .jackson[OptionalKV[Int, Chicken]](
            topic.sparKafka.params.pathBuilder(topic.topicName, Jackson))
          .typedDataset
          .collect[IO]()
          .map(r => assert(r.sorted.flatMap(_.value).toList == chickens))
    action.unsafeRunSync()
  }

  test("json") {
    val action =
      topic.sparKafka.fromKafka.flatMap(_.saveJson(blocker)).map(r => assert(r == 100)) >>
        sparkSession
          .json[OptionalKV[Int, Chicken]](topic.sparKafka.params.pathBuilder(topic.topicName, Json))
          .typedDataset
          .collect[IO]()
          .map(r => assert(r.sorted.flatMap(_.value).toList == chickens))
    action.unsafeRunSync()
  }

  test("avro") {

    val action =
      topic.sparKafka.fromKafka.flatMap(_.saveAvro(blocker)).map(r => assert(r == 100)) >>
        sparkSession
          .avro[OptionalKV[Int, Chicken]](topic.sparKafka.params.pathBuilder(topic.topicName, Avro))
          .collect[IO]()
          .map(r => assert(r.sorted.flatMap(_.value).toList == chickens))
    action.unsafeRunSync()
  }
  test("parquet") {
    val action =
      topic.sparKafka.fromKafka.flatMap(_.saveParquet(blocker)).map(r => assert(r == 100)) >>
        sparkSession
          .parquet[OptionalKV[Int, Chicken]](
            topic.sparKafka.params.pathBuilder(topic.topicName, Parquet))
          .collect[IO]()
          .map(r => assert(r.sorted.flatMap(_.value).toList == chickens))
    action.unsafeRunSync()
  }
}
