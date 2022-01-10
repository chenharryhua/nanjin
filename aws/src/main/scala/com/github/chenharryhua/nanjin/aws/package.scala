package com.github.chenharryhua.nanjin

import com.amazonaws.regions.Regions
import com.amazonaws.util.EC2MetadataUtils
import com.github.chenharryhua.nanjin.common.HostName

import scala.util.Try

package object aws {

  lazy val defaultRegion: Regions = Regions.AP_SOUTHEAST_2

  lazy val ec2_instance_id: HostName = HostName(Try(EC2MetadataUtils.getInstanceId).getOrElse("none"))

  lazy val ec2_private_ip: HostName = HostName(Try(EC2MetadataUtils.getPrivateIpAddress).getOrElse("none"))

}
