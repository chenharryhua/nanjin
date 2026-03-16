package com.github.chenharryhua.nanjin.aws

import com.github.chenharryhua.nanjin.aws.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import org.scalatest.funsuite.AnyFunSuite

class AwsArnTest extends AnyFunSuite {
  test("iam") {
    val iam: IamArn = "arn:aws:iam::123456789012:role/ab-c"
    println(iam)
  }

  test("sns") {
    val sns: SnsArn = "arn:aws:sns:ap-southeast-2:123456789012:abc-123xyz"
    println(sns)
  }

  test("cloudwatch namespace") {
    val cw: CloudWatchNs = "_-abc:213.33#"
    println(cw)
  }

  test("kms") {
    val kms: KmsArn = "arn:aws:kms:ap-southeast-2:123456789012:key/1111-2222-3333-13d5b006fbbb"
    println(kms)

  }

  test("sqs url") {
    val fifo: SqsUrl.Fifo = "https://github.com/abc.fifo"
    val std: SqsUrl.Standard = "https://github.com/abc"
    println(fifo)
    println(std)
  }

  test("address") {
    val address: Email = "who@gmail.com"
    println(address)
  }

  test("chunk.size") {
    val ck: ChunkSize = ChunkSize(100)
    println(ck)
  }
}
