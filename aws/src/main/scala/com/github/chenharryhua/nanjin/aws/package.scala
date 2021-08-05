package com.github.chenharryhua.nanjin

import com.amazonaws.regions.Regions
import com.amazonaws.util.EC2MetadataUtils
import com.github.chenharryhua.nanjin.common.HostName

package object aws {

  lazy val defaultRegion: Regions = Regions.AP_SOUTHEAST_2

  lazy val ec2_instance_id: HostName = new HostName {
    override val name: String = EC2MetadataUtils.getInstanceId
  }

  lazy val ec2_private_ip: HostName = new HostName {
    override val name: String = EC2MetadataUtils.getPrivateIpAddress
  }
}
