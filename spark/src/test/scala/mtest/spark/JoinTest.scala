package mtest.spark

import cats.effect.IO
import frameless.TypedDataset
import org.scalatest.funsuite.AnyFunSuite
import frameless.cats.implicits._

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
}

class JoinTest extends AnyFunSuite {
  import JoinTestData._
  test("spark left join") {
    val db = TypedDataset.create(brothers)
    val ds = TypedDataset.create(sisters)

    val res = db.joinLeft(ds)(db('id) === ds('id))
    res.show[IO](truncate = false).unsafeRunSync()

  }

}
