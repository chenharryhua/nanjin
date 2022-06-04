package mtest

import com.github.chenharryhua.nanjin.common.aws.SnsArn
import cron4s.Cron
import cron4s.expr.CronExpr

import java.time.ZoneId

package object guard {
  val snsArn: SnsArn              = SnsArn("arn:aws:sns:aaaa:123456789012:bb")
  final val trisecondly: CronExpr = Cron.unsafeParse("*/3 * * ? * *")
  final val hourly: CronExpr      = Cron.unsafeParse("0 0 0-23 ? * *")
  final val secondly: CronExpr    = Cron.unsafeParse("0-59 * * ? * *")
  final val beijingTime: ZoneId   = ZoneId.of("Asia/Shanghai")
}
