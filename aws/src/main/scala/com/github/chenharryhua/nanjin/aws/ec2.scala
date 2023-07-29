package com.github.chenharryhua.nanjin.aws

import com.github.chenharryhua.nanjin.common.HostName
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils

import scala.util.Try

object ec2 {
  lazy val instance_id: HostName =
    new HostName(Try(Option(EC2MetadataUtils.getInstanceId)).toOption.flatten.getOrElse("ec2.id.none"))

  lazy val private_ip: HostName =
    new HostName(Try(Option(EC2MetadataUtils.getPrivateIpAddress)).toOption.flatten.getOrElse("ec2.ip.none"))
}
