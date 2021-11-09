package mtest.spark.kafka

import cats.derived.auto.eq.kittensMkEq
import com.github.chenharryhua.nanjin.spark.kafka.*
import frameless.TypedDataset
import mtest.spark.sparkSession
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

object InvestigationTestData {
  final case class Mouse(size: Int, weight: Float)

  val mouses1: List[NJConsumerRecord[String, Mouse]] = List(
    NJConsumerRecord(0, 1, 10, Some("mike1"), Some(Mouse(1, 0.1f)), "topic", 0),
    NJConsumerRecord(0, 2, 20, Some("mike2"), Some(Mouse(2, 0.2f)), "topic", 0),
    NJConsumerRecord(0, 3, 30, Some("mike3"), Some(Mouse(3, 0.3f)), "topic", 0),
    NJConsumerRecord(1, 4, 40, Some("mike4"), Some(Mouse(4, 0.4f)), "topic", 0),
    NJConsumerRecord(1, 5, 50, Some("mike5"), Some(Mouse(5, 0.5f)), "topic", 0),
    NJConsumerRecord(1, 6, 60, Some("mike6"), Some(Mouse(6, 0.6f)), "topic", 0)
  )

  val mouses2: List[NJConsumerRecord[String, Mouse]] = List( // identical to mouse1
    NJConsumerRecord(0, 1, 10, Some("mike1"), Some(Mouse(1, 0.1f)), "topic", 0),
    NJConsumerRecord(0, 2, 20, Some("mike2"), Some(Mouse(2, 0.2f)), "topic", 0),
    NJConsumerRecord(0, 3, 30, Some("mike3"), Some(Mouse(3, 0.3f)), "topic", 0),
    NJConsumerRecord(1, 4, 40, Some("mike4"), Some(Mouse(4, 0.4f)), "topic", 0),
    NJConsumerRecord(1, 5, 50, Some("mike5"), Some(Mouse(5, 0.5f)), "topic", 0),
    NJConsumerRecord(1, 6, 60, Some("mike6"), Some(Mouse(6, 0.6f)), "topic", 0)
  )

  val mouses3: List[NJConsumerRecord[String, Mouse]] = List( // data diff (1,6) from mouse1
    NJConsumerRecord(0, 1, 10, Some("mike1"), Some(Mouse(1, 0.1f)), "topic", 0),
    NJConsumerRecord(0, 2, 20, Some("mike2"), Some(Mouse(2, 0.2f)), "topic", 0),
    NJConsumerRecord(0, 3, 30, Some("mike3"), Some(Mouse(3, 0.3f)), "topic", 0),
    NJConsumerRecord(1, 4, 40, Some("mike4"), Some(Mouse(4, 0.4f)), "topic", 0),
    NJConsumerRecord(1, 5, 50, Some("mike5"), Some(Mouse(5, 0.5f)), "topic", 0),
    NJConsumerRecord(1, 6, 60, Some("mike6"), Some(Mouse(6, 2.0f)), "topic", 0)
  )

  val mouses4: List[NJConsumerRecord[String, Mouse]] = List( // missing (1,5) from mouse1
    NJConsumerRecord(0, 1, 10, Some("mike1"), Some(Mouse(1, 0.1f)), "topic", 0),
    NJConsumerRecord(0, 2, 20, Some("mike2"), Some(Mouse(2, 0.2f)), "topic", 0),
    NJConsumerRecord(0, 3, 30, Some("mike3"), Some(Mouse(3, 0.3f)), "topic", 0),
    NJConsumerRecord(1, 4, 40, Some("mike4"), Some(Mouse(4, 0.4f)), "topic", 0),
    NJConsumerRecord(1, 6, 60, Some("mike6"), Some(Mouse(6, 0.6f)), "topic", 0)
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
  import InvestigationTestData.*
  implicit val ss: SparkSession = sparkSession

  test("sparKafka identical") {
    val m1 = TypedDataset.create(mouses1)
    val m2 = TypedDataset.create(mouses2)
    assert(0 === inv.diffDataset(m1, m2).dataset.count())
    assert(0 === inv.diffRdd(m1.dataset.rdd, m2.dataset.rdd).count())

    assert(0 === inv.kvDiffDataset(m1, m2).dataset.count())
    assert(0 === inv.kvDiffRdd(m1.dataset.rdd, m2.dataset.rdd).count())
  }

  test("sparKafka one mismatch") {
    val m1 = TypedDataset.create[NJConsumerRecord[String, Mouse]](mouses1)
    val m3 = TypedDataset.create[NJConsumerRecord[String, Mouse]](mouses3)

    val rst: Set[DiffResult[String, Mouse]] =
      inv.diffDataset(m1, m3).dataset.collect().toSet

    val rst2: Set[DiffResult[String, Mouse]] =
      inv.diffRdd(m1.dataset.rdd, m3.dataset.rdd).collect().toSet

    val tup: DiffResult[String, Mouse] = DiffResult(
      NJConsumerRecord(1, 6, 60, Some("mike6"), Some(Mouse(6, 0.6f)), "topic", 0),
      Some(NJConsumerRecord(1, 6, 60, Some("mike6"), Some(Mouse(6, 2.0f)), "topic", 0)))

    assert(Set(tup) == rst)
    assert(Set(tup) == rst2)

    val kv: Set[KvDiffResult[String, Mouse]] =
      Set(KvDiffResult(Some("mike6"), Some(Mouse(6, 0.6f))))
    val rst3: Set[KvDiffResult[String, Mouse]] =
      inv.kvDiffDataset(m1, m3).dataset.collect().toSet

    val rst4: Set[KvDiffResult[String, Mouse]] =
      inv.kvDiffRdd(m1.dataset.rdd, m3.dataset.rdd).collect().toSet
    assert(rst3 == kv)
    assert(rst4 == kv)
  }

  test("sparKafka one lost") {
    val m1 = TypedDataset.create[NJConsumerRecord[String, Mouse]](mouses1)
    val m4 = TypedDataset.create[NJConsumerRecord[String, Mouse]](mouses4)

    val rst: Set[DiffResult[String, Mouse]] =
      inv.diffDataset(m1, m4).dataset.collect().toSet
    val rst2: Set[DiffResult[String, Mouse]] =
      inv.diffRdd(m1.dataset.rdd, m4.dataset.rdd).collect().toSet
    val tup =
      DiffResult(NJConsumerRecord(1, 5, 50, Some("mike5"), Some(Mouse(5, 0.5f)), "topic", 0), None)
    assert(Set(tup) == rst)
    assert(Set(tup) == rst2)

    val rst3: Set[KvDiffResult[String, Mouse]] =
      inv.kvDiffDataset(m1, m4).dataset.collect().toSet
    val rst4: Set[KvDiffResult[String, Mouse]] =
      inv.kvDiffRdd(m1.dataset.rdd, m4.dataset.rdd).collect().toSet
    val kv = Set(KvDiffResult(Some("mike5"), Some(Mouse(5, 0.5f))))
    assert(rst3 == kv)
    assert(rst4 == kv)
  }

}
