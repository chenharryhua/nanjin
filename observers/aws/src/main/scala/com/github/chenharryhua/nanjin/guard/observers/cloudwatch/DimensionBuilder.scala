package com.github.chenharryhua.nanjin.guard.observers.cloudwatch

import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.translator.Attribute
import org.typelevel.cats.time.instances.localdate
import software.amazon.awssdk.services.cloudwatch.model.Dimension

import java.util
import scala.jdk.CollectionConverters.SeqHasAsJava

final class DimensionBuilder private[cloudwatch] (serviceParams: ServiceParams, map: Map[String, String])
    extends localdate {

  private def add(key: String, value: String): DimensionBuilder =
    new DimensionBuilder(serviceParams, map.updated(key, value))

  def withServiceName: DimensionBuilder = {
    val service_name = Attribute(serviceParams.serviceName).textEntry
    add(service_name.tag, service_name.text)
  }

  def withServiceID: DimensionBuilder = {
    val service_id = Attribute(serviceParams.serviceId).textEntry
    add(service_id.tag, service_id.text)
  }

  private[cloudwatch] def build: util.List[Dimension] =
    map.map { case (k, v) =>
      Dimension.builder().name(k).value(v).build()
    }.toList.asJava

}
