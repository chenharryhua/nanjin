package mtest.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.messages.kafka.{CRMetaInfo, NJConsumerRecord}
import com.github.chenharryhua.nanjin.spark.table.LoadTable
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, SparkSessionExt}
import mtest.spark.sparkSession
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

object InvestigationTestData {
  final case class Mouse(size: Int, weight: Float)

  val mouses1: List[NJConsumerRecord[String, Mouse]] = List(
    NJConsumerRecord("topic", 0, 1, 10, 0, None, None, Some("mike1"), Some(Mouse(1, 0.1f)), Nil, None),
    NJConsumerRecord("topic", 0, 2, 20, 0, None, None, Some("mike2"), Some(Mouse(2, 0.2f)), Nil, None),
    NJConsumerRecord("topic", 0, 3, 30, 0, None, None, Some("mike3"), Some(Mouse(3, 0.3f)), Nil, None),
    NJConsumerRecord("topic", 1, 4, 40, 0, None, None, Some("mike4"), Some(Mouse(4, 0.4f)), Nil, None),
    NJConsumerRecord("topic", 1, 5, 50, 0, None, None, Some("mike5"), Some(Mouse(5, 0.5f)), Nil, None),
    NJConsumerRecord("topic", 1, 6, 60, 0, None, None, Some("mike6"), Some(Mouse(6, 0.6f)), Nil, None)
  )

  val mouses2: List[NJConsumerRecord[String, Mouse]] = List( // identical to mouse1
    NJConsumerRecord("topic", 0, 1, 10, 0, None, None, Some("mike1"), Some(Mouse(1, 0.1f)), Nil, None),
    NJConsumerRecord("topic", 0, 2, 20, 0, None, None, Some("mike2"), Some(Mouse(2, 0.2f)), Nil, None),
    NJConsumerRecord("topic", 0, 3, 30, 0, None, None, Some("mike3"), Some(Mouse(3, 0.3f)), Nil, None),
    NJConsumerRecord("topic", 1, 4, 40, 0, None, None, Some("mike4"), Some(Mouse(4, 0.4f)), Nil, None),
    NJConsumerRecord("topic", 1, 5, 50, 0, None, None, Some("mike5"), Some(Mouse(5, 0.5f)), Nil, None),
    NJConsumerRecord("topic", 1, 6, 60, 0, None, None, Some("mike6"), Some(Mouse(6, 0.6f)), Nil, None)
  )

  val mouses3: List[NJConsumerRecord[String, Mouse]] = List( // data diff (1,6) from mouse1
    NJConsumerRecord("topic", 0, 1, 10, 0, None, None, Some("mike1"), Some(Mouse(1, 0.1f)), Nil, None),
    NJConsumerRecord("topic", 0, 2, 20, 0, None, None, Some("mike2"), Some(Mouse(2, 0.2f)), Nil, None),
    NJConsumerRecord("topic", 0, 3, 30, 0, None, None, Some("mike3"), Some(Mouse(3, 0.3f)), Nil, None),
    NJConsumerRecord("topic", 1, 4, 40, 0, None, None, Some("mike4"), Some(Mouse(4, 0.4f)), Nil, None),
    NJConsumerRecord("topic", 1, 5, 50, 0, None, None, Some("mike5"), Some(Mouse(5, 0.5f)), Nil, None),
    NJConsumerRecord("topic", 1, 6, 60, 0, None, None, Some("mike6"), Some(Mouse(6, 2.0f)), Nil, None)
  )

  val mouses4: List[NJConsumerRecord[String, Mouse]] = List( // missing (1,5) from mouse1
    NJConsumerRecord("topic", 0, 1, 10, 0, None, None, Some("mike1"), Some(Mouse(1, 0.1f)), Nil, None),
    NJConsumerRecord("topic", 0, 2, 20, 0, None, None, Some("mike2"), Some(Mouse(2, 0.2f)), Nil, None),
    NJConsumerRecord("topic", 0, 3, 30, 0, None, None, Some("mike3"), Some(Mouse(3, 0.3f)), Nil, None),
    NJConsumerRecord("topic", 1, 4, 40, 0, None, None, Some("mike4"), Some(Mouse(4, 0.4f)), Nil, None),
    NJConsumerRecord("topic", 1, 6, 60, 0, None, None, Some("mike6"), Some(Mouse(6, 0.6f)), Nil, None)
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
  val table: LoadTable[NJConsumerRecord[String, Mouse]] =
    ss.loadTable(AvroTypedEncoder[NJConsumerRecord[String, Mouse]])

  test("sparKafka identical") {
    val m1 = table.data(mouses1)
    val m2 = table.data(mouses2)
    assert(0 === m1.diff(m2).count[IO].unsafeRunSync())
  }

  test("sparKafka one mismatch") {
    val m1  = table.data(mouses1)
    val m3  = table.data(mouses3)
    val rst = m1.diff(m3).dataset.collect().toSet
    assert(
      rst === Set(
        NJConsumerRecord("topic", 1, 6, 60, 0, None, None, Some("mike6"), Some(Mouse(6, 0.6f)), Nil, None)))
  }

  test("sparKafka one lost") {
    val m1 = table.data(mouses1)
    val m4 = table.data(mouses4)

    assert(
      m1.diff(m4).dataset.collect().toSet === Set(
        NJConsumerRecord("topic", 1, 5, 50, 0, None, None, Some("mike5"), Some(Mouse(5, 0.5f)), Nil, None)))
  }
}
