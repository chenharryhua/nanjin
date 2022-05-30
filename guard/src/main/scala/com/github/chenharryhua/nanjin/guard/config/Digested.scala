package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import cats.data.NonEmptyList
import com.github.chenharryhua.nanjin.common.guard.Span
import io.circe.generic.JsonCodec
import io.circe.refined.*

@JsonCodec
final case class Digested private[config] (spans: NonEmptyList[Span], digest: String) {
  val origin: String     = spans.reverse.map(_.value).toList.mkString("/")
  val metricRepr: String = s"[$origin][$digest]"

  override val toString: String = metricRepr
}

object Digested {

  implicit val showDigestedName: Show[Digested] = _.metricRepr

}
