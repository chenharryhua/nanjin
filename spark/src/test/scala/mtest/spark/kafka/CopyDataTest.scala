package mtest.spark.kafka

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.spark.kafka._
import frameless.cats.implicits._
import org.scalatest.funsuite.AnyFunSuite

object CopyData {
  final case class MyTestData(a: Int, b: String)
}

class CopyDataTest extends AnyFunSuite {
  import CopyData._
  val src = ctx.topic[Int, MyTestData]("copy.src")
  val tgt = ctx.topic[Int, MyTestData]("copy.target")

  val d1 = src.fs2PR(0, MyTestData(1, "a")).withTimestamp(10)
  val d2 = src.fs2PR(1, MyTestData(2, "b")).withTimestamp(20)
  val d3 = src.fs2PR(2, MyTestData(3, "c")).withTimestamp(30)
  val d4 = src.fs2PR(null.asInstanceOf[Int], MyTestData(4, "d")).withTimestamp(40)
  val d5 = src.fs2PR(4, null.asInstanceOf[MyTestData]).withTimestamp(50)

  val prepareData =
    src.admin.IdefinitelyWantToDeleteTheTopic >> src.schemaRegistry.delete >> src.schemaRegistry.register >>
      tgt.admin.IdefinitelyWantToDeleteTheTopic >> tgt.schemaRegistry.delete >> tgt.schemaRegistry.register >>
      src.send(d1) >> src.send(d2) >> src.send(d3) >> src.send(d4) >> src.send(d5)

  test("pipeTo should copy data from source to target") {
    val rst = for {
      _ <- prepareData
      _ <- src.kit.sparKafka.pipeTo[IO](tgt.kit)
      srcData <- src.kit.sparKafka
        .fromKafka[IO]
        .flatMap(_.typedDataset.collect[IO].map(_.sortBy(_.timestamp)))
      tgtData <- tgt.kit.sparKafka
        .fromKafka[IO]
        .flatMap(_.typedDataset.collect[IO].map(_.sortBy(_.timestamp)))
    } yield {
      assert(srcData.size == 5)
      assert(tgtData.size == 4)
      assert(srcData.take(4).map(_.timestamp) == tgtData.map(_.timestamp))
      assert(srcData.take(4).map(_.value) === tgtData.map(_.value))
      assert(srcData.take(4).map(_.key) === tgtData.map(_.key))
    }

    rst.unsafeRunSync()
  }
}
