package mtest.spark.kafka

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import com.github.chenharryhua.nanjin.spark.kafka._
import fs2.kafka.ProducerRecord
import org.scalatest.funsuite.AnyFunSuite
import shapeless.{:+:, CNil, Coproduct}
import io.circe.generic.auto._
import io.circe.shapes._
import scala.util.Random

object MultiSaveTestData {
  final case class Apple(size: Int, locale: String)
  final case class WaterMelon(size: Int, weight: Float)
  type Fruit = Apple :+: WaterMelon :+: CNil
  final case class Food(fruit: Fruit, num: Int)

  val food = List(
    Food(Coproduct[Fruit](Apple(1, "qidong")), Random.nextInt),
    Food(Coproduct[Fruit](WaterMelon(2, 1.1f)), Random.nextInt),
    Food(Coproduct[Fruit](Apple(3, "hunan")), Random.nextInt),
    Food(Coproduct[Fruit](Apple(4, "chengdu")), Random.nextInt),
    Food(Coproduct[Fruit](WaterMelon(5, 1.1f)), Random.nextInt),
    Food(Coproduct[Fruit](Apple(6, "wuhan")), Random.nextInt),
    Food(Coproduct[Fruit](WaterMelon(7, 1.1f)), Random.nextInt),
    Food(Coproduct[Fruit](Apple(8, "chongqin")), Random.nextInt),
    Food(Coproduct[Fruit](WaterMelon(9, 1.1f)), Random.nextInt),
    Food(Coproduct[Fruit](Apple(10, "shanghai")), Random.nextInt)
  )

  val topic: KafkaTopic[IO, Int, Food] =
    TopicDef[Int, Food](TopicName("test.spark.kafka.multi.save")).in(ctx)

  val prs: List[ProducerRecord[Int, Food]] = food.map(f => topic.fs2PR(0, f))

}

class MultiSaveTest extends AnyFunSuite {
  import MultiSaveTestData._

  (topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
    topic.send(prs)).unsafeRunSync()

  test("multi-save avro") {
    val rst = topic.sparKafka.fromKafka.flatMap(_.repartition(3).save.multi(blocker).avro) >> IO {
      topic.sparKafka.read.multi.avro.rdd.collect.flatMap(_.value).toSet
    }
    assert(rst.unsafeRunSync() == food.toSet)
  }
  test("multi-save jackson") {
    val rst = topic.sparKafka.fromKafka.flatMap(_.repartition(3).save.multi(blocker).jackson) >> IO {
      topic.sparKafka.read.multi.jackson.rdd.collect().flatMap(_.value).toSet
    }
    assert(rst.unsafeRunSync() == food.toSet)
  }
}
