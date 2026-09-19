package com.github.chenharryhua.nanjin.common.chrono

import cats.Show
import cats.syntax.show.showInterpolator
import higherkindness.droste.data.Fix
import higherkindness.droste.{scheme, Algebra}
import org.typelevel.cats.time.instances.duration.given
import org.typelevel.cats.time.instances.localtime.given

private object ShowPolicy {
  import PolicyF.*
  private val showPolicy: Algebra[PolicyF, String] = Algebra[PolicyF, String] {
    case Empty() => show"$EMPTY"

    case Crontab(cronExpr) => show"$CRONTAB($cronExpr)"

    case FixedDelay(delays) => show"$FIXED_DELAY(${delays.toList.mkString(",")})"
    case FixedRate(delay)   => show"$FIXED_RATE($delay)"

    // ops
    case Limited(policy, limit)       => show"$policy.$LIMITED($limit)"
    case FollowedBy(leader, follower) => show"$leader.$FOLLOWED_BY($follower)"
    case Repeat(policy)               => show"$policy.$REPEAT"
    case Meet(first, second)          => show"$first.$MEET($second)"
    case Except(policy, except)       => show"$policy.$EXCEPT($except)"
    case Offset(policy, offset)       => show"$policy.$OFFSET($offset)"
    case Jitter(policy, min, max)     => show"$policy.$JITTER($min,$max)"
  }

  def apply(policy: Fix[PolicyF]): String =
    scheme.cata(showPolicy).apply(policy)
}
