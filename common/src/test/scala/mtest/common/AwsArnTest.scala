package mtest.common

import com.github.chenharryhua.nanjin.common.aws.*
import org.scalatest.funsuite.AnyFunSuite
class AwsArnTest extends AnyFunSuite {
  test("iam") {
    IamArn.unsafeFrom("arn:aws:iam::123456789012:role/ab-c")
  }

  test("sns") {
    SnsArn.unsafeFrom("arn:aws:sns:ap-southeast-2:123456789012:abc-123xyz")
  }

  test("cloudwatch namespace") {
    CloudWatchNamespace.unsafeFrom("_-abc:213.33#")
  }

  test("kms") {
    KmsArn.unsafeFrom("arn:aws:kms:ap-southeast-2:123456789012:key/1111-2222-3333-13d5b006fbbb")

  }

  test("sqs url") {

    SqsUrl.Fifo.unsafeFrom("https://github.com/abc.fifo")
    SqsUrl.Standard.unsafeFrom("https://github.com/abc")

  }
}
