package com.github.chenharryhua.nanjin.aws

import com.amazonaws.util.EC2MetadataUtils
import com.github.chenharryhua.nanjin.common.HostName

import scala.util.Try

object ec2 {
  lazy val instance_id: HostName =
    HostName(Try(Option(EC2MetadataUtils.getInstanceId)).toOption.flatten.getOrElse("ec2.id.none"))

  lazy val private_ip: HostName =
    HostName(Try(Option(EC2MetadataUtils.getPrivateIpAddress)).toOption.flatten.getOrElse("ec2.ip.none"))
}
