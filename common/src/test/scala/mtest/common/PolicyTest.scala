package mtest.common

import com.github.chenharryhua.nanjin.common.chrono.{lazyTickList, policies}
import cron4s.Cron
import org.scalatest.funsuite.AnyFunSuite

class PolicyTest extends AnyFunSuite {

  test("policy") {
    val policy =
      policies
        .crontab(Cron.unsafeParse("0 0 0-8 ? * *"))
        .meet(policies.crontab(Cron.unsafeParse("0 */5 9-17 ? * *")))
        .meet(policies.crontab(Cron.unsafeParse("0 0 18-23 ? * *"))).except(_.noon)

    val ts = zeroTickStatus.renewPolicy(policy)
    lazyTickList(ts)
      .take(123)
      .toList
      .foreach(println)
  }
}
