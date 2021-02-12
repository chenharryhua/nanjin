package mtest.spark.kafka

import cats.syntax.all._
import frameless.cats.implicits._
import fs2.kafka.{ProducerRecord, ProducerRecords}
import mtest.spark.{contextShift, timer}
import org.scalatest.funsuite.AnyFunSuite

object CopyData {
  final case class MyTestData(a: Int, b: String)
}

class CopyDataTest extends AnyFunSuite {
  import CopyData._
  val src = ctx.topic[Int, MyTestData]("copy.src")
  val tgt = ctx.topic[Int, MyTestData]("copy.target")

  val d1 = ProducerRecord(src.topicName.value, 0, MyTestData(1, "a")).withTimestamp(10)
  val d2 = ProducerRecord(src.topicName.value, 1, MyTestData(2, "b")).withTimestamp(20)
  val d3 = ProducerRecord(src.topicName.value, 2, MyTestData(3, "c")).withTimestamp(30)
  val d4 = ProducerRecord(src.topicName.value, null.asInstanceOf[Int], MyTestData(4, "d")).withTimestamp(40)
  val d5 = ProducerRecord(src.topicName.value, 4, null.asInstanceOf[MyTestData]).withTimestamp(50)

  val loadData =
    fs2.Stream(ProducerRecords(List(d1, d2, d3, d4, d5))).covary.through(src.fs2Channel.producerPipe).compile.drain

  val prepareData =
    src.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      src.schemaRegistry.delete >>
      src.schemaRegistry.register >>
      tgt.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      tgt.schemaRegistry.delete >>
      tgt.schemaRegistry.register >>
      loadData

  test("sparKafka pipeTo should copy data from source to target") {
    val rst = for {
      _ <- prepareData
      _ <- sparKafka.topic(src.topicDef).fromKafka.prRdd.byBatch.toTopic(tgt).upload.compile.drain
    } yield {
      val srcData = sparKafka.topic(src.topicDef).fromKafka.rdd.collect
      val tgtData = sparKafka.topic(tgt.topicDef).fromKafka.rdd.collect
      assert(srcData.size == 5)
      assert(tgtData.size == 5)
      assert(srcData.map(_.value).toSet === tgtData.map(_.value).toSet)
      assert(srcData.map(_.key).toSet === tgtData.map(_.key).toSet)
    }
    rst.unsafeRunSync()
  }
}
