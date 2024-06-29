package mtest.common

import com.github.chenharryhua.nanjin.common.chrono.{policies, tickLazyList, TickStatus}
import cron4s.Cron
import org.scalatest.funsuite.AnyFunSuite

class PolicyTest extends AnyFunSuite {

  test("policy") {
    val policy =
      policies
        .crontab(Cron.unsafeParse("0 0 0-10,13-23 ? * *"))
        .meet(policies.crontab(Cron.unsafeParse("0 */5 11,12 ? * *")))
        .except(_.noon)

    val ts = zeroTickStatus.renewPolicy(policy)
    tickLazyList(ts).take(10).foreach { t =>
      println(t.zonedPrevious)
    }
  }

  test("policy 2") {
    val policy = policies
      .accordance(policies.crontab(Cron.unsafeParse("0 0 0 * 1-11 ?")))
      .meet(policies.crontab(Cron.unsafeParse("0 0 0 1-25 12 ?")))

    val ts: TickStatus = zeroTickStatus.renewPolicy(policy)
    tickLazyList(ts).slice(320, 365).foreach(println)
  }
}
