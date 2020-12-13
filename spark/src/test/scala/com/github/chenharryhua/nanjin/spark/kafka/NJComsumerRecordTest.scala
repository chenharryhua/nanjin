package com.github.chenharryhua.nanjin.spark.kafka

import cats.kernel.laws.discipline.{LowerBoundedTests, OrderTests}
import cats.laws.discipline.{BifunctorTests, BitraverseTests}
import cats.tests.CatsSuite
import org.scalacheck.{Arbitrary, Cogen, Gen}
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

class NJComsumerRecordTest extends CatsSuite with FunSuiteDiscipline {

  implicit val ocogen  = Cogen[OptionalKV[Int, Int]]((o: OptionalKV[Int, Int]) => o.offset)
  implicit val kcogen  = Cogen[CompulsoryK[Int, Int]]((o: CompulsoryK[Int, Int]) => o.offset)
  implicit val vcogen  = Cogen[CompulsoryV[Int, Int]]((o: CompulsoryV[Int, Int]) => o.offset)
  implicit val kvcogen = Cogen[CompulsoryKV[Int, Int]]((o: CompulsoryKV[Int, Int]) => o.offset)

  implicit val prcogen =
    Cogen[NJProducerRecord[Int, Int]]((o: NJProducerRecord[Int, Int]) => o.timestamp.getOrElse(0L))

  val kv: Gen[CompulsoryKV[Int, Int]] = for {
    partition <- Gen.posNum[Int]
    offset <- Gen.posNum[Long]
    timestamp <- Gen.posNum[Long]
    timestampType <- Gen.oneOf(List(0, 1))
  } yield CompulsoryKV(partition, offset, timestamp, 0, 1, "topic", timestampType)

  implicit val arbKV = Arbitrary(kv)
  implicit val arbO  = Arbitrary(kv.map(_.toOptionalKV))
  implicit val arbK  = Arbitrary(kv.map(_.toCompulsoryK))
  implicit val arbV  = Arbitrary(kv.map(_.toCompulsoryV))
  implicit val arbP  = Arbitrary(kv.map(_.toOptionalKV.toNJProducerRecord))

  // ordered
  checkAll("OptionalKV", OrderTests[OptionalKV[Int, Int]].order)
  checkAll("OptionalKV", LowerBoundedTests[OptionalKV[Int, Int]].lowerBounded)

  checkAll("CompulsoryK", OrderTests[CompulsoryK[Int, Int]].order)
  checkAll("CompulsoryV", OrderTests[CompulsoryV[Int, Int]].order)
  checkAll("CompulsoryKV", OrderTests[CompulsoryKV[Int, Int]].order)

  // bitraverse
  checkAll(
    "CompulsoryKV",
    BitraverseTests[CompulsoryKV].bitraverse[Option, Int, Int, Int, Int, Int, Int])

  // bifunctor
  checkAll("CompulsoryK", BifunctorTests[CompulsoryK].bifunctor[Int, Int, Int, Int, Int, Int])
  checkAll("CompulsoryV", BifunctorTests[CompulsoryV].bifunctor[Int, Int, Int, Int, Int, Int])
  checkAll("OptionalKV", BifunctorTests[OptionalKV].bifunctor[Int, Int, Int, Int, Int, Int])
  checkAll(
    "NJProducerRecord",
    BifunctorTests[NJProducerRecord].bifunctor[Int, Int, Int, Int, Int, Int])

}
