package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import cats.data.NonEmptyList
import com.github.chenharryhua.nanjin.common.guard.{ServiceName, Span}
import io.circe.generic.JsonCodec
import io.circe.refined.*

@JsonCodec
final case class Digested private (spans: NonEmptyList[Span], digest: String) {
  val origin: String     = spans.map(_.value).toList.mkString("/")
  val metricRepr: String = s"[$origin][$digest]"

  override val toString: String = metricRepr
}

private[guard] object Digested {

  implicit val showDigestedName: Show[Digested] = _.metricRepr

  def apply(spans: NonEmptyList[Span], serviceParams: ServiceParams): Digested = {
    val digest: String = digestSpans(spans.prepend(serviceParams.taskParams.taskName))
    new Digested(spans, digest)
  }

  def apply(serviceName: ServiceName, taskParams: TaskParams): Digested = {
    val digest = digestSpans(NonEmptyList.of(serviceName, taskParams.taskName)) // order matters
    new Digested(NonEmptyList.one(serviceName), digest)
  }
}
