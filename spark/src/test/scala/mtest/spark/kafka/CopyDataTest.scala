package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.TopicDef
import eu.timepit.refined.auto.*
import fs2.kafka.{ProducerRecord, ProducerRecords}
import org.scalatest.funsuite.AnyFunSuite

object CopyData {
  final case class MyTestData(a: Int, b: String)
}

class CopyDataTest extends AnyFunSuite {
  import CopyData.*
  val td = TopicDef[Int, MyTestData](TopicName("tn"))
  val src = td.withTopicName("copy.src")
  val tgt = td.withTopicName("copy.target")

  val d1 = ProducerRecord(src.topicName.value, 0, MyTestData(1, "a")).withTimestamp(10)
  val d2 = ProducerRecord(src.topicName.value, 1, MyTestData(2, "b")).withTimestamp(20)
  val d3 = ProducerRecord(src.topicName.value, 2, MyTestData(3, "c")).withTimestamp(30)
  val d4 = ProducerRecord(src.topicName.value, null.asInstanceOf[Int], MyTestData(4, "d")).withTimestamp(40)
  val d5 = ProducerRecord(src.topicName.value, 4, null.asInstanceOf[MyTestData]).withTimestamp(50)

  val loadData: IO[Unit] =
    fs2
      .Stream(ProducerRecords(List(d1, d2, d3, d4, d5)))
      .covary[IO]
      .through(ctx.produce[Int, MyTestData].sink)
      .compile
      .drain

  val prepareData =
    ctx.admin(src.topicName.name).use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt) >>
      ctx.schemaRegistry.delete(src.topicName).attempt >>
      ctx.schemaRegistry.register(src).attempt >>
      ctx
        .admin(tgt.topicName.name)
        .use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt) >>
      ctx.schemaRegistry.delete(tgt.topicName).attempt >>
      ctx.schemaRegistry.register(tgt).attempt >>
      loadData

  test("sparKafka pipeTo should copy data from source to target") {
    val rst = for {
      _ <- prepareData
      _ <- sparKafka
        .topic(src)
        .fromKafka
        .flatMap(
          _.prRdd.noPartition.noTimestamp.noMeta
            .withTopicName(tgt.topicName)
            .producerRecords[IO](100)
            .through(ctx.produce[Int, MyTestData].sink)
            .compile
            .drain)
      srcData = sparKafka.topic(src).fromKafka.map(_.rdd.collect()).unsafeRunSync()
      tgtData = sparKafka.topic(tgt).fromKafka.map(_.rdd.collect()).unsafeRunSync()
    } yield {

      assert(srcData.length == 5)
      assert(tgtData.length == 5)
      assert(srcData.map(_.value).toSet === tgtData.map(_.value).toSet)
      assert(srcData.map(_.key).toSet === tgtData.map(_.key).toSet)
    }
    rst.unsafeRunSync()
  }
}
