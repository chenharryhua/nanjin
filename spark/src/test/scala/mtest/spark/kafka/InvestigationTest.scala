package mtest.spark.kafka

import cats.derived.auto.eq.kittensMkEq
import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.kafka.{
  inv,
  CRMetaInfo,
  DiffResult,
  KvDiffResult,
  OptionalKV
}
import frameless.TypedDataset
import frameless.cats.implicits._
import mtest.spark.{contextShift, sparkSession}
import org.scalatest.funsuite.AnyFunSuite

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
    CRMetaInfo("topic", 0, 1, 1, 0),
    CRMetaInfo("topic", 0, 2, 2, 0),
    CRMetaInfo("topic", 0, 3, 3, 0),
    CRMetaInfo("topic", 1, 4, 4, 0),
    CRMetaInfo("topic", 1, 6, 6, 0)
  )

  val mouses6 = List( // (0,2) duplicate
    CRMetaInfo("topic", 0, 1, 1, 0),
    CRMetaInfo("topic", 0, 2, 2, 0),
    CRMetaInfo("topic", 0, 2, 3, 0),
    CRMetaInfo("topic", 0, 2, 4, 0),
    CRMetaInfo("topic", 1, 5, 6, 0)
  )

}

class InvestigationTest extends AnyFunSuite {
  import InvestigationTestData._

  test("sparKafka identical") {
    val m1 = TypedDataset.create(mouses1)
    val m2 = TypedDataset.create(mouses2)
    assert(0 === inv.diffDataset(m1, m2).count[IO]().unsafeRunSync())
    assert(0 === inv.diffRdd(m1.dataset.rdd, m2.dataset.rdd).count())

    assert(0 === inv.kvDiffDataset(m1, m2).count[IO]().unsafeRunSync())
    assert(0 === inv.kvDiffRdd(m1.dataset.rdd, m2.dataset.rdd).count())
  }

  test("sparKafka one mismatch") {
    val m1 = TypedDataset.create[OptionalKV[String, Mouse]](mouses1)
    val m3 = TypedDataset.create[OptionalKV[String, Mouse]](mouses3)

    val rst: Set[DiffResult[String, Mouse]] =
      inv.diffDataset(m1, m3).collect[IO]().unsafeRunSync().toSet

    val rst2: Set[DiffResult[String, Mouse]] =
      inv.diffRdd(m1.dataset.rdd, m3.dataset.rdd).collect().toSet

    val tup: DiffResult[String, Mouse] = DiffResult(
      OptionalKV(1, 6, 60, Some("mike6"), Some(Mouse(6, 0.6f)), "topic", 0),
      Some(OptionalKV(1, 6, 60, Some("mike6"), Some(Mouse(6, 2.0f)), "topic", 0)))

    assert(Set(tup) == rst)
    assert(Set(tup) == rst2)

    val kv: Set[KvDiffResult[String, Mouse]] =
      Set(KvDiffResult(Some("mike6"), Some(Mouse(6, 0.6f))))
    val rst3: Set[KvDiffResult[String, Mouse]] =
      inv.kvDiffDataset(m1, m3).collect[IO]().unsafeRunSync().toSet

    val rst4: Set[KvDiffResult[String, Mouse]] =
      inv.kvDiffRdd(m1.dataset.rdd, m3.dataset.rdd).collect().toSet
    assert(rst3 == kv)
    assert(rst4 == kv)
  }

  test("sparKafka one lost") {
    val m1 = TypedDataset.create[OptionalKV[String, Mouse]](mouses1)
    val m4 = TypedDataset.create[OptionalKV[String, Mouse]](mouses4)

    val rst: Set[DiffResult[String, Mouse]] =
      inv.diffDataset(m1, m4).collect[IO]().unsafeRunSync().toSet
    val rst2: Set[DiffResult[String, Mouse]] =
      inv.diffRdd(m1.dataset.rdd, m4.dataset.rdd).collect().toSet
    val tup =
      DiffResult(OptionalKV(1, 5, 50, Some("mike5"), Some(Mouse(5, 0.5f)), "topic", 0), None)
    assert(Set(tup) == rst)
    assert(Set(tup) == rst2)

    val rst3: Set[KvDiffResult[String, Mouse]] =
      inv.kvDiffDataset(m1, m4).collect[IO]().unsafeRunSync().toSet
    val rst4: Set[KvDiffResult[String, Mouse]] =
      inv.kvDiffRdd(m1.dataset.rdd, m4.dataset.rdd).collect().toSet
    val kv = Set(KvDiffResult(Some("mike5"), Some(Mouse(5, 0.5f))))
    assert(rst3 == kv)
    assert(rst4 == kv)
  }

}
