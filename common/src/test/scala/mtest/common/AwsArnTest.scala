package mtest.common

import com.github.chenharryhua.nanjin.common.aws.*
import org.scalatest.funsuite.AnyFunSuite

class AwsArnTest extends AnyFunSuite {
  test("iam") {
    IamArn("arn:aws:iam::123456789012:role/ab-c")
  }

  test("sns") {
    SnsArn("arn:aws:sns:ap-southeast-2:123456789012:abc-123xyz")
  }

  test("cloudwatch namespace") {
    CloudWatchNamespace("_-abc:213.33#")
    shapeless.test.illTyped(""" CloudWatchNamespace("a\b") """)
  }

  test("kms") {
    KmsArn("arn:aws:kms:ap-southeast-2:123456789012:key/1111-2222-3333-13d5b006fbbb")

  }

  test("sqs url") {

    SqsUrl.Fifo("https://github.com/abc.fifo")
    SqsUrl.Standard("https://github.com/abc")

    shapeless.test.illTyped(""" SqsUrl.Fifo("https://github.com/abc") """)
    shapeless.test.illTyped(""" SqsUrl.Standard("https://github.com/abc.fifo") """)

  }
}
