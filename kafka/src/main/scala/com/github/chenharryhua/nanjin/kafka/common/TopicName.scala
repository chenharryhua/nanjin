package com.github.chenharryhua.nanjin.kafka.common

import cats.Show
import cats.implicits._
import cats.kernel.Eq
import eu.timepit.refined.api.Refined
import eu.timepit.refined.refineV
import eu.timepit.refined.string.MatchesRegex
import shapeless.Witness

final class TopicName(name: Refined[String, TopicName.Constraint]) extends Serializable {
  val value: String             = name.value
  override val toString: String = value
}

object TopicName {
  type Constraint = MatchesRegex[Witness.`"^[a-zA-Z0-9_.-]+$"`.T]

  implicit val eqshowTopicName: Eq[TopicName] with Show[TopicName] =
    new Eq[TopicName] with Show[TopicName] {

      override def eqv(x: TopicName, y: TopicName): Boolean =
        x.value === y.value

      override def show(t: TopicName): String =
        t.value
    }

  @throws[Exception]
  def apply(name: String): TopicName =
    refineV[Constraint](name).map(new TopicName(_)).fold(e => throw new Exception(e), identity)
}
