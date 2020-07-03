package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.kafka.{inv, CRMetaInfo, DiffResult, DupResult}
import frameless.TypedDataset
import org.scalatest.funsuite.AnyFunSuite
import frameless.cats.implicits._
import cats.derived.auto.eq.kittensMkEq
import cats.implicits._

object InvestigationTestData {
  final case class Mouse(size: Int, weight: Float)

  val mouses1: List[OptionalKV[String, Mouse]] = List(
    OptionalKV(0, 1, 10, Some("mike1"), Some(Mouse(1, 0.1f)), "topic", 0),
    OptionalKV(0, 2, 20, Some("mike2"), Some(Mouse(2, 0.2f)), "topic", 0),
    OptionalKV(0, 3, 30, Some("mike3"), Some(Mouse(3, 0.3f)), "topic", 0),
    OptionalKV(1, 4, 40, Some("mike4"), Some(Mouse(4, 0.4f)), "topic", 0),
    OptionalKV(1, 5, 50, Some("mike5"), Some(Mouse(5, 0.5f)), "topic", 0),
    OptionalKV(1, 6, 60, Some("mike6"), Some(Mouse(6, 0.6f)), "topic", 0)
  )

  val mouses2: List[OptionalKV[String, Mouse]] = List( // identical to mouse1
    OptionalKV(0, 1, 10, Some("mike1"), Some(Mouse(1, 0.1f)), "topic", 0),
    OptionalKV(0, 2, 20, Some("mike2"), Some(Mouse(2, 0.2f)), "topic", 0),
    OptionalKV(0, 3, 30, Some("mike3"), Some(Mouse(3, 0.3f)), "topic", 0),
    OptionalKV(1, 4, 40, Some("mike4"), Some(Mouse(4, 0.4f)), "topic", 0),
    OptionalKV(1, 5, 50, Some("mike5"), Some(Mouse(5, 0.5f)), "topic", 0),
    OptionalKV(1, 6, 60, Some("mike6"), Some(Mouse(6, 0.6f)), "topic", 0)
  )

  val mouses3: List[OptionalKV[String, Mouse]] = List( // data diff (1,6) from mouse1
    OptionalKV(0, 1, 10, Some("mike1"), Some(Mouse(1, 0.1f)), "topic", 0),
    OptionalKV(0, 2, 20, Some("mike2"), Some(Mouse(2, 0.2f)), "topic", 0),
    OptionalKV(0, 3, 30, Some("mike3"), Some(Mouse(3, 0.3f)), "topic", 0),
    OptionalKV(1, 4, 40, Some("mike4"), Some(Mouse(4, 0.4f)), "topic", 0),
    OptionalKV(1, 5, 50, Some("mike5"), Some(Mouse(5, 0.5f)), "topic", 0),
    OptionalKV(1, 6, 60, Some("mike6"), Some(Mouse(6, 2.0f)), "topic", 0)
  )

  val mouses4: List[OptionalKV[String, Mouse]] = List( // missing (1,5) from mouse1
    OptionalKV(0, 1, 10, Some("mike1"), Some(Mouse(1, 0.1f)), "topic", 0),
    OptionalKV(0, 2, 20, Some("mike2"), Some(Mouse(2, 0.2f)), "topic", 0),
    OptionalKV(0, 3, 30, Some("mike3"), Some(Mouse(3, 0.3f)), "topic", 0),
    OptionalKV(1, 4, 40, Some("mike4"), Some(Mouse(4, 0.4f)), "topic", 0),
    OptionalKV(1, 6, 60, Some("mike6"), Some(Mouse(6, 0.6f)), "topic", 0)
  )

  val mouses5 = List( // missing (1,5)
    CRMetaInfo("topic", 0, 1, 1),
    CRMetaInfo("topic", 0, 2, 2),
    CRMetaInfo("topic", 0, 3, 3),
    CRMetaInfo("topic", 1, 4, 4),
    CRMetaInfo("topic", 1, 6, 6))

  val mouses6 = List( // (0,2) duplicate
    CRMetaInfo("topic", 0, 1, 1),
    CRMetaInfo("topic", 0, 2, 2),
    CRMetaInfo("topic", 0, 2, 3),
    CRMetaInfo("topic", 0, 2, 4),
    CRMetaInfo("topic", 1, 5, 6))

}

class InvestigationTest extends AnyFunSuite {
  import InvestigationTestData._

  test("identical") {

    assert(
      0 === inv
        .diffDataset(TypedDataset.create(mouses1), TypedDataset.create(mouses2))
        .count[IO]()
        .unsafeRunSync())

  }

  test("one mismatch") {
    val rst: Set[DiffResult[String, Mouse]] = inv
      .diffDataset(
        TypedDataset.create[OptionalKV[String, Mouse]](mouses1),
        TypedDataset.create[OptionalKV[String, Mouse]](mouses3))
      .collect[IO]()
      .unsafeRunSync()
      .toSet

    val tup = DiffResult(
      OptionalKV(1, 6, 60, Some("mike6"), Some(Mouse(6, 0.6f)), "topic", 0),
      Some(OptionalKV(1, 6, 60, Some("mike6"), Some(Mouse(6, 2.0f)), "topic", 0)))

    assert(Set(tup) == rst)

  }

  test("one lost") {
    val rst: Set[DiffResult[String, Mouse]] = inv
      .diffDataset(
        TypedDataset.create[OptionalKV[String, Mouse]](mouses1),
        TypedDataset.create[OptionalKV[String, Mouse]](mouses4))
      .collect[IO]()
      .unsafeRunSync()
      .toSet
    val tup =
      DiffResult(OptionalKV(1, 5, 50, Some("mike5"), Some(Mouse(5, 0.5f)), "topic", 0), None)
    assert(Set(tup) == rst)
  }

  test("missing data") {
    assert(
      Set(CRMetaInfo("topic", 1, 4, 4)) ==
        inv.missingData(TypedDataset.create(mouses5)).collect[IO]().unsafeRunSync().toSet)

  }

  test("duplicate") {
    val rst: Set[DupResult] =
      inv.dupRecords(TypedDataset.create(mouses6)).collect[IO]().unsafeRunSync().toSet
    assert(Set(DupResult(0, 2, 3)) == rst)
  }
}
