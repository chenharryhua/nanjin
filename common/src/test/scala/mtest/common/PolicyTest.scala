package mtest.common

import com.github.chenharryhua.nanjin.common.chrono.{tickLazyList, Policy}
import cron4s.Cron
import org.scalatest.funsuite.AnyFunSuite

class PolicyTest extends AnyFunSuite {

  test("policy") {
    val policy =
      Policy
        .crontab(Cron.unsafeParse("0 0 0-10,13-23 ? * *"))
        .meet(Policy.crontab(Cron.unsafeParse("0 */5 11,12 ? * *")))
        .except(_.noon)

    tickLazyList(policy).take(50).foreach(println)
  }

}
