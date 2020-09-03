package mtest.spark.kafka

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.TopicName
import com.github.chenharryhua.nanjin.spark.kafka._
import frameless.cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.kafka.TopicDef

object CopyData {
  final case class MyTestData(a: Int, b: String)
}

class CopyDataTest extends AnyFunSuite {
  import CopyData._
  val src = TopicDef[Int, MyTestData](TopicName("copy.src")).in(ctx)
  val tgt = TopicDef[Int, MyTestData](TopicName("copy.target")).in(ctx)

  val d1 = src.fs2PR(0, MyTestData(1, "a")).withTimestamp(10)
  val d2 = src.fs2PR(1, MyTestData(2, "b")).withTimestamp(20)
  val d3 = src.fs2PR(2, MyTestData(3, "c")).withTimestamp(30)
  val d4 = src.fs2PR(null.asInstanceOf[Int], MyTestData(4, "d")).withTimestamp(40)
  val d5 = src.fs2PR(4, null.asInstanceOf[MyTestData]).withTimestamp(50)

  val prepareData =
    src.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >> src.schemaDelete >> src.schemaRegister >>
      tgt.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >> tgt.schemaDelete >> tgt.schemaRegister >>
      src.send(d1) >> src.send(d2) >> src.send(d3) >> src.send(d4) >> src.send(d5)

  test("sparKafka pipeTo should copy data from source to target") {
    val rst = for {
      _ <- prepareData
      _ <- src.sparKafka(range).pipeTo(tgt)
      srcData <- src.sparKafka(range).fromKafka.flatMap(_.crDataset.typedDataset.collect[IO])
      tgtData <- tgt.sparKafka(range).fromKafka.flatMap(_.crDataset.typedDataset.collect[IO])
    } yield {
      assert(srcData.size == 5)
      assert(tgtData.size == 5)
      assert(srcData.map(_.value).toSet === tgtData.map(_.value).toSet)
      assert(srcData.map(_.key).toSet === tgtData.map(_.key).toSet)
    }

    rst.unsafeRunSync()
  }
}
