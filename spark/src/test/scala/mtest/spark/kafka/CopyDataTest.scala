package mtest.spark.kafka

import cats.syntax.all._
import frameless.cats.implicits._
import mtest.spark.{contextShift, timer}
import org.scalatest.funsuite.AnyFunSuite

object CopyData {
  final case class MyTestData(a: Int, b: String)
}

class CopyDataTest extends AnyFunSuite {
  import CopyData._
  val src = ctx.topic[Int, MyTestData]("copy.src")
  val tgt = ctx.topic[Int, MyTestData]("copy.target")

  val d1 = src.producerRecord(0, MyTestData(1, "a")).withTimestamp(10)
  val d2 = src.producerRecord(1, MyTestData(2, "b")).withTimestamp(20)
  val d3 = src.producerRecord(2, MyTestData(3, "c")).withTimestamp(30)
  val d4 = src.producerRecord(null.asInstanceOf[Int], MyTestData(4, "d")).withTimestamp(40)
  val d5 = src.producerRecord(4, null.asInstanceOf[MyTestData]).withTimestamp(50)

  val prepareData =
    src.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      src.schemaRegistry.delete >>
      src.schemaRegistry.register >>
      tgt.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >>
      tgt.schemaRegistry.delete >>
      tgt.schemaRegistry.register >>
      src.send(d1) >> src.send(d2) >> src.send(d3) >> src.send(d4) >> src.send(d5)

  test("sparKafka pipeTo should copy data from source to target") {
    val rst = for {
      _ <- prepareData
      _ <- sparKafka.topic(src.topicDef).fromKafka.prRdd.map(identity)(tgt).upload.compile.drain
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
