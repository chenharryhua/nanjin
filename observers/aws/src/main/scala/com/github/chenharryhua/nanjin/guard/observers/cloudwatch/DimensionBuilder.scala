package com.github.chenharryhua.nanjin.guard.observers.cloudwatch

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.translator.textConstants
import org.typelevel.cats.time.instances.localdate
import software.amazon.awssdk.services.cloudwatch.model.Dimension

import java.util
import scala.jdk.CollectionConverters.SeqHasAsJava

final class DimensionBuilder private[cloudwatch] (serviceParams: ServiceParams, map: Map[String, String])
    extends localdate {

  private def add(key: String, value: String): DimensionBuilder =
    new DimensionBuilder(serviceParams, map.updated(key, value))

  def withTaskName: DimensionBuilder =
    add(textConstants.CONSTANT_TASK, serviceParams.taskName.value)

  def withHostName: DimensionBuilder =
    add(textConstants.CONSTANT_HOST, serviceParams.host.name.value)

  def withLaunchDate: DimensionBuilder =
    add(textConstants.CONSTANT_LAUNCH_TIME, serviceParams.launchTime.toLocalDate.show)

  def withServiceName: DimensionBuilder =
    add(textConstants.CONSTANT_SERVICE, serviceParams.serviceName.value)

  def withServiceID: DimensionBuilder =
    add(textConstants.CONSTANT_SERVICE_ID, serviceParams.serviceId.show)

  private[cloudwatch] def build: util.List[Dimension] =
    map.map { case (k, v) =>
      Dimension.builder().name(k).value(v).build()
    }.toList.asJava

}
