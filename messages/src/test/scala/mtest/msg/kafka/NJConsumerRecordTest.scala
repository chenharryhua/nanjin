package mtest.msg.kafka
import cats.laws.discipline.BifunctorTests
import cats.tests.CatsSuite
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

object NJConsumerRecordTestData {
  implicit val ocogen: Cogen[NJConsumerRecord[Int, Int]] =
    Cogen[NJConsumerRecord[Int, Int]]((o: NJConsumerRecord[Int, Int]) => o.offset)

  implicit val prcogen: Cogen[NJProducerRecord[Int, Int]] =
    Cogen[NJProducerRecord[Int, Int]]((o: NJProducerRecord[Int, Int]) => o.timestamp.getOrElse(0L))

  val okv: Gen[NJConsumerRecord[Int, Int]] = for {
    partition <- Gen.posNum[Int]
    offset <- Gen.posNum[Long]
    timestamp <- Gen.posNum[Long]
    timestampType <- Gen.oneOf(List(0, 1))
    k <- Gen.option(Gen.posNum[Int])
    v <- Gen.option(Gen.posNum[Int])
  } yield NJConsumerRecord("topic", partition, offset, timestamp, timestampType, Nil, None, -1, -1, k, v)

  val genPR: Gen[NJProducerRecord[Int, Int]] = for {
    partition <- Gen.option(Gen.posNum[Int])
    timestamp <- Gen.option(Gen.posNum[Long])
    k <- Gen.option(Gen.posNum[Int])
    v <- Gen.option(Gen.posNum[Int])
  } yield NJProducerRecord("topic", partition, None, timestamp, Nil, k, v)

  implicit val arbPR: Arbitrary[NJProducerRecord[Int, Int]] = Arbitrary(genPR)
  implicit val arbO: Arbitrary[NJConsumerRecord[Int, Int]] = Arbitrary(okv)
}

class NJConsumerRecordTest extends CatsSuite with FunSuiteDiscipline {
  import NJConsumerRecordTestData.*

  // partial ordered
  // checkAll("OptionalKV", PartialOrderTests[NJConsumerRecord[Int, Int]].partialOrder)

  // bifunctor
  checkAll("OptionalKV", BifunctorTests[NJConsumerRecord].bifunctor[Int, Int, Int, Int, Int, Int])
  checkAll("NJProducerRecord", BifunctorTests[NJProducerRecord].bifunctor[Int, Int, Int, Int, Int, Int])

}
