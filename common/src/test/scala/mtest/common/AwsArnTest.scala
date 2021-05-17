package mtest.common

import com.github.chenharryhua.nanjin.common.aws.{IamArn, SnsArn, SqsUrl}
import org.scalatest.funsuite.AnyFunSuite

class AwsArnTest extends AnyFunSuite {
  test("iam") {
    IamArn("arn:aws:iam::123456789012:role/abc")
  }
  test("sns") {
    SnsArn("arn:aws:sns:ap-southeast-2:123456789012:abc-123_xyz")
  }
  test("sqs") {
    SqsUrl("https://github.com")
    SqsUrl("http://github.com")
    shapeless.test.illTyped("""SqsUrl("abc")""")
  }
}
