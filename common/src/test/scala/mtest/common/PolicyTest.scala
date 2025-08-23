package mtest.common

import com.github.chenharryhua.nanjin.common.chrono.{tickLazyList, Policy}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class PolicyTest extends AnyFunSuite {

  test("policy") {
    val policy =
      Policy.crontab(_.every5Minutes).jitter(30.seconds)

    tickLazyList.fromOne(policy).take(50).foreach(println)
  }

}
