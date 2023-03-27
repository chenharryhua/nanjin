package com.github.chenharryhua.nanjin.guard.service

import com.codahale.metrics.jmx.ObjectNameFactory
import com.github.chenharryhua.nanjin.guard.config.MetricID
import io.circe.parser.decode

import java.util
import javax.management.ObjectName

private object objectNameFactory extends ObjectNameFactory {
  override def createName(tipe: String, domain: String, name: String): ObjectName =
    decode[MetricID](name) match {
      case Left(ex) => throw ex
      case Right(mId) =>
        val properties = new util.Hashtable[String, String]()
        properties.put("name", mId.metricName.value)
        properties.put("type", mId.category.name)
        properties.put("digest", mId.metricName.digest.value)
        val dm = s"$domain.${mId.metricName.measurement.value}"
        new ObjectName(dm, properties)
    }
}
