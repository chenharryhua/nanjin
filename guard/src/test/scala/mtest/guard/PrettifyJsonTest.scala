package mtest.guard

import com.github.chenharryhua.nanjin.guard.action.{Detail, QuasiResult}
import com.github.chenharryhua.nanjin.guard.translator.prettifyJson
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.ScalaDurationOps
import io.circe.jawn.parse
class PrettifyJsonTest extends AnyFunSuite {
  test("prettify case class") {
    val qr = QuasiResult(
      "123",
      "sequential",
      List(Detail(1, 2.seconds.toJava, is_done = true), Detail(2, 1.second.toJava, is_done = false)))
    val res = prettifyJson(qr)
    val json =
      """
        |{
        |  "token" : "123",
        |  "mode" : "sequential",
        |  "details" : [
        |    {
        |      "nth_job" : "1",
        |      "took" : "2 seconds",
        |      "is_done" : true
        |    },
        |    {
        |      "nth_job" : "2",
        |      "took" : "1 second",
        |      "is_done" : false
        |    }
        |  ]
        |}
        |""".stripMargin

    assert(parse(json).toOption.get == res)
  }
}
