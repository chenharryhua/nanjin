package com.github.chenharryhua.nanjin.guard.service

import com.codahale.metrics.jmx.ObjectNameFactory
import com.github.chenharryhua.nanjin.guard.event.MetricID
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
