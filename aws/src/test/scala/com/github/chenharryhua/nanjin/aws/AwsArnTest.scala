package com.github.chenharryhua.nanjin.aws

import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.aws.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import io.circe.{Decoder, Encoder}
import org.scalatest.funsuite.AnyFunSuite
import io.circe.syntax.EncoderOps

class AwsArnTest extends AnyFunSuite {
  test("iam") {
    val iam: IamArn = "arn:aws:iam::123456789012:role/ab-c"
    println(iam.asJson)
    println(iam.show)
  }

  test("sns") {
    val sns: SnsArn = "arn:aws:sns:ap-southeast-2:123456789012:abc-123xyz"
    println(sns.asJson)
    println(sns.show)
  }

  test("cloudwatch namespace") {
    val cw: CloudWatchNs = "_-abc:213.33#"
    println(cw.asJson)
    println(cw.show)
  }

  test("kms") {
    val kms: KmsArn = "arn:aws:kms:ap-southeast-2:123456789012:key/1111-2222-3333-13d5b006fbbb"
    println(kms.asJson)
    println(kms.show)

  }

  test("sqs url") {
    val fifo: SqsUrl.Fifo = "https://github.com/abc.fifo"
    val std: SqsUrl.Standard = "https://github.com/abc"
    println(fifo.asJson)
    println(std.show)
  }

  test("address") {
    val address: Email = "who@gmail.com"
    println(address.asJson)
    println(address.show)
  }

  test("chunk.size") {
    val ck: ChunkSize = ChunkSize(100)
    println(ck.asJson)
    println(ck.show)
  }

  test("email content") {
    summon[Encoder[EmailContent]]
    summon[Decoder[EmailContent]]
  }
}
