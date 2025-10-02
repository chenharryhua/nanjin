package mtest.common

import com.github.chenharryhua.nanjin.common.aws.*
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*

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
    val cwn: CloudWatchNamespace = "_-abc:213.33#"
    println(cwn)

    shapeless.test.illTyped(""" CloudWatchNamespace("a\b") """)
  }

  test("kms") {
    val kms: KmsArn = "arn:aws:kms:ap-southeast-2:123456789012:key/1111-2222-3333-13d5b006fbbb"
    println(kms)
  }

  test("sqs url") {

    val fifo: SqsUrl.Fifo = "https://github.com/abc.fifo"
    val standard: SqsUrl.Standard = "https://github.com/abc"
    println((fifo, standard))

    shapeless.test.illTyped(""" SqsUrl.Fifo("https://github.com/abc") """)
    shapeless.test.illTyped(""" SqsUrl.Standard("https://github.com/abc.fifo") """)

  }
}
