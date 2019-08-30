package com.github.chenharryhua.nanjin.kafka

import cats.Eq
import cats.effect.IO
import org.scalatest.{FunSuite, TestRegistration}
import cats.derived.auto.show._
import cats.implicits._
import monocle.law.discipline.IsoTests
import org.typelevel.discipline.scalatest.Discipline

class IsoTest extends Discipline with TestRegistration {
  implicit val eqArrayByte: Eq[Array[Byte]] = ??? // cats.derived.semi.eq[Array[Byte]]

  val topic: KafkaTopic[IO, Int, Payment] = TopicDef[Int, Payment]("payment").in(ctx)

  checkAll("iso", IsoTests(topic.keyIso))

}
