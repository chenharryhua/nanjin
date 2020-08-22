package mtest.spark

import cats.effect.IO
import cats.kernel.Eq
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.cats.implicits._
import frameless.{TypedDataset, TypedEncoder}
import mtest.spark.pb.test.Whale
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite
import cats.derived.auto.eq._
import cats.implicits._

import scala.util.Random

object JoinTestData {
  final case class Brother(id: Int, rel: String)
  final case class Sister(id: Int, sibling: String)

  val brothers = List(
    Brother(1, "a"),
    Brother(1, "b"),
    Brother(2, "c")
  )

  val sisters = List(
    Sister(1, "x"),
    Sister(1, "y"),
    Sister(3, "z")
  )

  object Fruit extends Enumeration {
    val Apple, Watermelon = Value
  }

  final case class Food(kind: Fruit.Value, num: Int)

  val shouldCompile: Eq[Food] = implicitly[Eq[Food]]
}

class MiscTest extends AnyFunSuite {
  import JoinTestData._
  test("spark left join") {
    val db = TypedDataset.create(brothers)
    val ds = TypedDataset.create(sisters)

    val res = db.joinLeft(ds)(db('id) === ds('id))
    res.show[IO](truncate = false).unsafeRunSync()

  }

  test("typed encoder of scalapb generate case class") {
    import scalapb.spark.Implicits._
    val pt: TypedEncoder[Whale] = TypedEncoder[Whale] // should compile
    val whales = List(
      Whale("aaa", Random.nextInt()),
      Whale("bbb", Random.nextInt()),
      Whale("ccc", Random.nextInt())
    )
    val path = "./data/test/spark/protobuf/whales.json"
    TypedDataset.create(whales).write.mode(SaveMode.Overwrite).parquet(path)
    val rst =
      TypedDataset
        .createUnsafe[Whale](sparkSession.read.parquet(path))
        .collect[IO]
        .unsafeRunSync()
        .toSet
    assert(rst == whales.toSet)
  }

  test("gen case class") {
    val ds = TypedDataset.create(sisters)
    println(ds.dataset.toDF().genCaseClass)
    println(ds.dataset.toDF().genSchema)
  }
}
