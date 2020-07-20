package mtest.spark.kafka

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat.{Avro, Jackson, Json, Parquet}
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.kafka._
import frameless.cats.implicits._
import fs2.kafka.ProducerRecord
import io.circe.generic.auto._
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object SaveTestData {
  final case class Chicken(a: Int, b: String)

  val topic: KafkaTopic[IO, Int, Chicken] =
    TopicDef[Int, Chicken](TopicName("test.spark.kafka.save.load")).in(ctx)

}

class SaveTest extends AnyFunSuite {
  import SaveTestData._

  val sk: SparKafka[IO, Int, Chicken] = topic.sparKafka(range)

  val chickenPR: List[ProducerRecord[Int, Chicken]] =
    List.fill(100)(topic.fs2PR(Random.nextInt(), Chicken(Random.nextInt(), "aaa")))
  val chickens: List[Chicken] = chickenPR.map(_.value)

  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
    topic.send(chickenPR)).unsafeRunSync()

  test("sparKafka dump") {
    topic.sparKafka(range).dump.unsafeRunSync()
  }
  test("sparKafka jackson") {

    val action =
      sk.fromKafka.flatMap(_.saveJackson(blocker)).map(r => assert(r == 100)) >>
        sparkSession
          .jackson[OptionalKV[Int, Chicken]](sk.params.pathBuilder(topic.topicName, Jackson))
          .typedDataset
          .collect[IO]()
          .map(r => assert(r.sorted.flatMap(_.value).toList == chickens))
    action.unsafeRunSync()
  }

  test("sparKafka json") {
    val action =
      sk.fromKafka.flatMap(_.saveJson(blocker)).map(r => assert(r == 100)) >>
        sparkSession
          .json[OptionalKV[Int, Chicken]](sk.params.pathBuilder(topic.topicName, Json))
          .typedDataset
          .collect[IO]()
          .map(r => assert(r.sorted.flatMap(_.value).toList == chickens))
    action.unsafeRunSync()
  }

  test("sparKafka avro") {

    val action = sk.fromKafka.flatMap(_.saveAvro(blocker)).map(r => assert(r == 100))
    action.unsafeRunSync()
    val rst: Array[OptionalKV[Int, Chicken]] = sparkSession
      .avro[OptionalKV[Int, Chicken]](sk.params.pathBuilder(topic.topicName, Avro))
      .collect()
      .sorted
    assert(rst.flatMap(_.value).toList == chickens)
  }
  test("sparKafka parquet") {
    val action = sk.fromKafka.flatMap(_.saveParquet(blocker)).map(r => assert(r == 100))
    action.unsafeRunSync()
    val rst = sparkSession
      .parquet[OptionalKV[Int, Chicken]](sk.params.pathBuilder(topic.topicName, Parquet))
      .collect()
      .sorted
    assert(rst.flatMap(_.value).toList == chickens)
  }
}
