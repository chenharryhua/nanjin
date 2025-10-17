package mtest.spark.kafka

import cats.implicits.catsSyntaxOptionId
import com.github.chenharryhua.nanjin.messages.kafka.{MetaInfo, NJConsumerRecord}
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import mtest.spark.sparkSession
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

object InvestigationTestData {
  final case class Mouse(size: Int, weight: Float)

  val mouses1: List[NJConsumerRecord[String, Mouse]] = List(
    NJConsumerRecord("topic", 0, 1, 10, 0, Nil, None, None, None, Some("mike1"), Some(Mouse(1, 0.1f))),
    NJConsumerRecord("topic", 0, 2, 20, 0, Nil, None, None, None, Some("mike2"), Some(Mouse(2, 0.2f))),
    NJConsumerRecord("topic", 0, 3, 30, 0, Nil, None, None, None, Some("mike3"), Some(Mouse(3, 0.3f))),
    NJConsumerRecord("topic", 1, 4, 40, 0, Nil, None, None, None, Some("mike4"), Some(Mouse(4, 0.4f))),
    NJConsumerRecord("topic", 1, 5, 50, 0, Nil, None, None, None, Some("mike5"), Some(Mouse(5, 0.5f))),
    NJConsumerRecord("topic", 1, 6, 60, 0, Nil, None, None, None, Some("mike6"), Some(Mouse(6, 0.6f)))
  )

  val mouses2: List[NJConsumerRecord[String, Mouse]] = List( // identical to mouse1
    NJConsumerRecord("topic", 0, 1, 10, 0, Nil, None, None, None, Some("mike1"), Some(Mouse(1, 0.1f))),
    NJConsumerRecord("topic", 0, 2, 20, 0, Nil, None, None, None, Some("mike2"), Some(Mouse(2, 0.2f))),
    NJConsumerRecord("topic", 0, 3, 30, 0, Nil, None, None, None, Some("mike3"), Some(Mouse(3, 0.3f))),
    NJConsumerRecord("topic", 1, 4, 40, 0, Nil, None, None, None, Some("mike4"), Some(Mouse(4, 0.4f))),
    NJConsumerRecord("topic", 1, 5, 50, 0, Nil, None, None, None, Some("mike5"), Some(Mouse(5, 0.5f))),
    NJConsumerRecord("topic", 1, 6, 60, 0, Nil, None, None, None, Some("mike6"), Some(Mouse(6, 0.6f)))
  )

  val mouses3: List[NJConsumerRecord[String, Mouse]] = List( // data diff (1,6) from mouse1
    NJConsumerRecord("topic", 0, 1, 10, 0, Nil, None, None, None, Some("mike1"), Some(Mouse(1, 0.1f))),
    NJConsumerRecord("topic", 0, 2, 20, 0, Nil, None, None, None, Some("mike2"), Some(Mouse(2, 0.2f))),
    NJConsumerRecord("topic", 0, 3, 30, 0, Nil, None, None, None, Some("mike3"), Some(Mouse(3, 0.3f))),
    NJConsumerRecord("topic", 1, 4, 40, 0, Nil, None, None, None, Some("mike4"), Some(Mouse(4, 0.4f))),
    NJConsumerRecord("topic", 1, 5, 50, 0, Nil, None, None, None, Some("mike5"), Some(Mouse(5, 0.5f))),
    NJConsumerRecord("topic", 1, 6, 60, 0, Nil, None, None, None, Some("mike6"), Some(Mouse(6, 2.0f)))
  )

  val mouses4: List[NJConsumerRecord[String, Mouse]] = List( // missing (1,5) from mouse1
    NJConsumerRecord("topic", 0, 1, 10, 0, Nil, None, None, None, Some("mike1"), Some(Mouse(1, 0.1f))),
    NJConsumerRecord("topic", 0, 2, 20, 0, Nil, None, None, None, Some("mike2"), Some(Mouse(2, 0.2f))),
    NJConsumerRecord("topic", 0, 3, 30, 0, Nil, None, None, None, Some("mike3"), Some(Mouse(3, 0.3f))),
    NJConsumerRecord("topic", 1, 4, 40, 0, Nil, None, None, None, Some("mike4"), Some(Mouse(4, 0.4f))),
    NJConsumerRecord("topic", 1, 6, 60, 0, Nil, None, None, None, Some("mike6"), Some(Mouse(6, 0.6f)))
  )

  val mouses5 = List( // missing (1,5)
    MetaInfo("topic", 0, 1, 1, 0.some, None, None),
    MetaInfo("topic", 0, 2, 2, 0.some, None, None),
    MetaInfo("topic", 0, 3, 3, 0.some, None, None),
    MetaInfo("topic", 1, 4, 4, 0.some, None, None),
    MetaInfo("topic", 1, 6, 6, 0.some, None, None)
  )

  val mouses6 = List( // (0,2) duplicate
    MetaInfo("topic", 0, 1, 1, 0.some, None, None),
    MetaInfo("topic", 0, 2, 2, 0.some, None, None),
    MetaInfo("topic", 0, 2, 3, 0.some, None, None),
    MetaInfo("topic", 0, 2, 4, 0.some, None, None),
    MetaInfo("topic", 1, 5, 6, 0.some, None, None)
  )

}

import mtest.spark.sparkSession.implicits.*

class InvestigationTest extends AnyFunSuite {
  import InvestigationTestData.*
  implicit val ss: SparkSession = sparkSession

  test("sparKafka identical") {
    val m1 = ss.loadData(mouses1)
    val m2 = ss.loadData(mouses2)
    assert(0 === m1.except(m2).count())
  }

  test("sparKafka one mismatch") {
    val m1 = ss.loadData(mouses1)
    val m3 = ss.loadData(mouses3)
    val rst = m1.except(m3).collect().toSet
    assert(
      rst === Set(
        NJConsumerRecord("topic", 1, 6, 60, 0, Nil, None, None, None, Some("mike6"), Some(Mouse(6, 0.6f)))))
  }

  test("sparKafka one lost") {
    val m1 = ss.loadData(mouses1)
    val m4 = ss.loadData(mouses4)

    assert(
      m1.except(m4).collect().toSet === Set(
        NJConsumerRecord("topic", 1, 5, 50, 0, Nil, None, None, None, Some("mike5"), Some(Mouse(5, 0.5f)))))
  }
}
