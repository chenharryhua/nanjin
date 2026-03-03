package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.Async
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jmx.{JmxReporter, ObjectNameFactory}
import com.github.chenharryhua.nanjin.guard.event.MetricID
import fs2.Stream
import io.circe.jawn.decode

import java.util
import javax.management.ObjectName
private object objectNameFactory extends ObjectNameFactory {
  override def createName(tipe: String, domain: String, name: String): ObjectName =
    decode[MetricID](name).map { mid =>
      val properties = new util.Hashtable[String, String]()
      properties.put("label", mid.metricLabel.label)
      properties.put("name", mid.metricName.name)
      properties.put("type", mid.category.productPrefix)
      val dm = s"$domain.${mid.metricLabel.domain.value}"
      new ObjectName(dm, properties)
    }.toOption.orNull
}

private object jmxReporter {

  def apply[F[_]](metricRegistry: MetricRegistry, jmxBuilder: Option[Endo[JmxReporter.Builder]])(implicit
    F: Async[F]): Stream[F, Nothing] =
    jmxBuilder match {
      case None        => Stream.empty
      case Some(build) =>
        Stream.bracket(F.blocking {
          val reporter =
            build(JmxReporter.forRegistry(metricRegistry)) // use home-brew factory
              .createsObjectNamesWith(objectNameFactory)
              .build()
          reporter.start()
          reporter
        })(r => F.blocking(r.stop())) >>
          Stream.never[F]
    }
}
