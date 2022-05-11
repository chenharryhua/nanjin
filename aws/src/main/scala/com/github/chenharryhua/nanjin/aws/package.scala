package com.github.chenharryhua.nanjin

import com.amazonaws.util.EC2MetadataUtils
import com.github.chenharryhua.nanjin.common.HostName

import scala.util.Try

package object aws {

  lazy val ec2_instance_id: HostName =
    HostName(Try(Option(EC2MetadataUtils.getInstanceId)).toOption.flatten.getOrElse("none"))

  lazy val ec2_private_ip: HostName =
    HostName(Try(Option(EC2MetadataUtils.getPrivateIpAddress)).toOption.flatten.getOrElse("none"))

}
