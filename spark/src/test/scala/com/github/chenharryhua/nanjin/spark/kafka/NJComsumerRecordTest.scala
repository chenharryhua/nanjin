package com.github.chenharryhua.nanjin.spark.kafka

import cats.derived.auto.eq.*
import cats.kernel.laws.discipline.PartialOrderTests
import cats.laws.discipline.BifunctorTests
import cats.tests.CatsSuite
import eu.timepit.refined.auto.*
import org.scalacheck.{Arbitrary, Cogen, Gen, Properties}
import org.typelevel.discipline.scalatest.FunSuiteDiscipline

object NJComsumerRecordTestData {
  implicit val ocogen = Cogen[NJConsumerRecord[Int, Int]]((o: NJConsumerRecord[Int, Int]) => o.offset)

  implicit val prcogen =
    Cogen[NJProducerRecord[Int, Int]]((o: NJProducerRecord[Int, Int]) => o.timestamp.getOrElse(0L))

  val okv: Gen[NJConsumerRecord[Int, Int]] = for {
    partition <- Gen.posNum[Int]
    offset <- Gen.posNum[Long]
    timestamp <- Gen.posNum[Long]
    timestampType <- Gen.oneOf(List(0, 1))
    k <- Gen.option(Gen.posNum[Int])
    v <- Gen.option(Gen.posNum[Int])
  } yield NJConsumerRecord(partition, offset, timestamp, k, v, "topic", timestampType)

  val genPR: Gen[NJProducerRecord[Int, Int]] = for {
    partition <- Gen.option(Gen.posNum[Int])
    timestamp <- Gen.option(Gen.posNum[Long])
    k <- Gen.option(Gen.posNum[Int])
    v <- Gen.option(Gen.posNum[Int])
  } yield NJProducerRecord(partition, None, timestamp, k, v)

  implicit val arbPR = Arbitrary(genPR)
  implicit val arbO  = Arbitrary(okv)
}

class NJComsumerRecordTest extends CatsSuite with FunSuiteDiscipline {
  import NJComsumerRecordTestData.*

  // partial ordered
  checkAll("OptionalKV", PartialOrderTests[NJConsumerRecord[Int, Int]].partialOrder)

  // bifunctor
  checkAll("OptionalKV", BifunctorTests[NJConsumerRecord].bifunctor[Int, Int, Int, Int, Int, Int])
  checkAll("NJProducerRecord", BifunctorTests[NJProducerRecord].bifunctor[Int, Int, Int, Int, Int, Int])

}

class NJComsumerRecordProp extends Properties("ConsumerRecord") {
  import NJComsumerRecordTestData.*
  import org.scalacheck.Prop.forAll

  property("fs2.producer.record.conversion") = forAll { (op: NJProducerRecord[Int, Int]) =>
    val fpr = op.toFs2ProducerRecord("topic")
    val re  = NJProducerRecord[Int, Int](fpr.partition, None, fpr.timestamp, Option(fpr.key), Option(fpr.value))
    re == op
  }
}
