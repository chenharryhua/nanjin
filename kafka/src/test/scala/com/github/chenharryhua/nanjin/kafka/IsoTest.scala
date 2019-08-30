package com.github.chenharryhua.nanjin.kafka

import cats.Eq
import cats.implicits._
import monocle.law.discipline.PrismTests
import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline

class IsoTest extends AnyFunSuite with Discipline {

  implicit val eqArrayByte: Eq[Array[Byte]] = (x: Array[Byte], y: Array[Byte]) =>
    x.zip(y).forall { case (x, y) => x.eqv(y) }

  val topic = ctx.topic[Int, Long]("payload")

  checkAll("prism-key", PrismTests(topic.keyPrism))
  checkAll("prism-value", PrismTests(topic.valuePrism))

}
