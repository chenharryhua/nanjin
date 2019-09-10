package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.codec.{LikeConsumerRecord, MessagePropertiesFs2}
import fs2.kafka.CommittableConsumerRecord
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline
import monocle.law.discipline.LensTests
import org.scalacheck.Arbitrary
import cats.implicits._
import org.apache.kafka.clients.consumer.ConsumerRecord

class LensTest extends AnyFunSuite with Discipline with Fs2MessageGen {
  implicit val fs2CM: Arbitrary[CommittableConsumerRecord[IO, Int, Int]] =
    Arbitrary(genFs2ConsumerMessage)

  implicit val cm: Arbitrary[ConsumerRecord[Int, Int]] =
    Arbitrary(genConsumerRecord)
  implicit val cmF = Arbitrary((cm: ConsumerRecord[Int, Int]) => cm)

  implicit val fs2CRLens =
    LikeConsumerRecord[CommittableConsumerRecord[IO, *, *]].lens[Int, Int, Int, Int]

  checkAll("fs2.CommittableConsumerRecord", LensTests(fs2CRLens))

}
