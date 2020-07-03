package mtest.spark.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.kafka.{
  inv,
  CRMetaInfo,
  DiffResult,
  DupResult,
  KafkaMsgDigest
}
import frameless.TypedDataset
import org.scalatest.funsuite.AnyFunSuite
import frameless.cats.implicits._

object InvestigationTestData {
  final case class Mouse(size: Int, weight: Float)

  val mouses1 = List(
    KafkaMsgDigest(0, 1, "mike1".hashCode, Mouse(1, 0.1f).hashCode()),
    KafkaMsgDigest(0, 2, "mike2".hashCode, Mouse(2, 0.2f).hashCode()),
    KafkaMsgDigest(0, 3, "mike3".hashCode, Mouse(3, 0.3f).hashCode()),
    KafkaMsgDigest(1, 4, "mike4".hashCode, Mouse(4, 0.4f).hashCode()),
    KafkaMsgDigest(1, 5, "mike5".hashCode, Mouse(5, 0.5f).hashCode()),
    KafkaMsgDigest(1, 6, "mike6".hashCode, Mouse(6, 0.6f).hashCode())
  )

  val mouses2 = List( // identical to mouse1
    KafkaMsgDigest(0, 1, "mike1".hashCode, Mouse(1, 0.1f).hashCode()),
    KafkaMsgDigest(0, 2, "mike2".hashCode, Mouse(2, 0.2f).hashCode()),
    KafkaMsgDigest(0, 3, "mike3".hashCode, Mouse(3, 0.3f).hashCode()),
    KafkaMsgDigest(1, 4, "mike4".hashCode, Mouse(4, 0.4f).hashCode()),
    KafkaMsgDigest(1, 5, "mike5".hashCode, Mouse(5, 0.5f).hashCode()),
    KafkaMsgDigest(1, 6, "mike6".hashCode, Mouse(6, 0.6f).hashCode())
  )

  val mouses3 = List( // data diff (1,6) from mouse1
    KafkaMsgDigest(0, 1, "mike1".hashCode, Mouse(1, 0.1f).hashCode()),
    KafkaMsgDigest(0, 2, "mike2".hashCode, Mouse(2, 0.2f).hashCode()),
    KafkaMsgDigest(0, 3, "mike3".hashCode, Mouse(3, 0.3f).hashCode()),
    KafkaMsgDigest(1, 4, "mike4".hashCode, Mouse(4, 0.4f).hashCode()),
    KafkaMsgDigest(1, 5, "mike5".hashCode, Mouse(5, 0.5f).hashCode()),
    KafkaMsgDigest(1, 6, "mike6".hashCode, Mouse(6, 2.0f).hashCode())
  )

  val mouses4 = List( // missing (1,5) from mouse1
    KafkaMsgDigest(0, 1, "mike1".hashCode, Mouse(1, 0.1f).hashCode()),
    KafkaMsgDigest(0, 2, "mike2".hashCode, Mouse(2, 0.2f).hashCode()),
    KafkaMsgDigest(0, 3, "mike3".hashCode, Mouse(3, 0.3f).hashCode()),
    KafkaMsgDigest(1, 4, "mike4".hashCode, Mouse(4, 0.4f).hashCode()),
    KafkaMsgDigest(1, 6, "mike6".hashCode, Mouse(6, 0.6f).hashCode())
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
    val rst: Set[DiffResult] = inv
      .diffDataset(TypedDataset.create(mouses1), TypedDataset.create(mouses3))
      .collect[IO]()
      .unsafeRunSync()
      .toSet

    val tup = DiffResult(
      KafkaMsgDigest(1, 6, "mike6".hashCode, Mouse(6, 0.6f).hashCode()),
      Some(KafkaMsgDigest(1, 6, "mike6".hashCode, Mouse(6, 2.0f).hashCode())))

    assert(Set(tup) === rst)

  }

  test("one lost") {
    val rst: Set[DiffResult] = inv
      .diffDataset(TypedDataset.create(mouses1), TypedDataset.create(mouses4))
      .collect[IO]()
      .unsafeRunSync()
      .toSet
    val tup = DiffResult(KafkaMsgDigest(1, 5, "mike5".hashCode, Mouse(5, 0.5f).hashCode()), None)
    assert(Set(tup) === rst)
  }

  test("missing data") {
    assert(
      Set(CRMetaInfo("topic", 1, 4, 4)) ===
        inv.missingData(TypedDataset.create(mouses5)).collect[IO]().unsafeRunSync().toSet)

  }

  test("duplicate") {
    val rst: Set[DupResult] =
      inv.dupRecords(TypedDataset.create(mouses6)).collect[IO]().unsafeRunSync().toSet
    assert(Set(DupResult(0, 2, 3)) === rst)
  }
}
