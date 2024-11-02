package mtest.common

import com.github.chenharryhua.nanjin.common.chrono.{Policy, tickLazyList}
import org.scalatest.funsuite.AnyFunSuite

class PolicyTest extends AnyFunSuite {

  test("policy") {
    val policy =
      Policy.crontab(_.monthly)

    tickLazyList(policy).take(50).foreach(println)
  }

}
