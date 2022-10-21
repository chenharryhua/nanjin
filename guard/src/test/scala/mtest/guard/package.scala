package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import cron4s.Cron
import cron4s.expr.CronExpr
import retry.RetryPolicies

import java.time.ZoneId
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

package object guard {
  val snsArn: SnsArn              = SnsArn("arn:aws:sns:aaaa:123456789012:bb")
  final val trisecondly: CronExpr = Cron.unsafeParse("*/3 * * ? * *")
  final val hourly: CronExpr      = Cron.unsafeParse("0 0 0-23 ? * *")
  final val secondly: CronExpr    = Cron.unsafeParse("0-59 * * ? * *")
  final val bisecondly: CronExpr  = Cron.unsafeParse("*/2 * * ? * *")
  val minutely: CronExpr          = Cron.unsafeParse("0 0-59 * ? * *")
  final val beijingTime: ZoneId   = ZoneId.of("Asia/Shanghai")

  def unit_fun: IO[Unit] = IO(())

  def add_fun(a: Int, b: Int): IO[Int] = IO(a + b)

  def err_fun(i: Int): IO[Int] = IO.raiseError[Int](new Exception(s"oops: $i"))

  def never_fun: IO[Int] = IO.never[Int]

  def fun1(i: Int)                                 = IO(i + 1)
  def fun2(a: Int, b: Int)                         = IO(a + b)
  def fun3(a: Int, b: Int, c: Int)                 = IO(a + b + c)
  def fun4(a: Int, b: Int, c: Int, d: Int)         = IO(a + b + c + d)
  def fun5(a: Int, b: Int, c: Int, d: Int, e: Int) = IO(a + b + c + d + e)

  def fun0fut: IO[Future[Int]]                        = IO(Future(1))
  def fun1fut(i: Int)                                 = Future(i + 1)
  def fun2fut(a: Int, b: Int)                         = Future(a + b)
  def fun3fut(a: Int, b: Int, c: Int)                 = Future(a + b + c)
  def fun4fut(a: Int, b: Int, c: Int, d: Int)         = Future(a + b + c + d)
  def fun5fut(a: Int, b: Int, c: Int, d: Int, e: Int) = Future(a + b + c + d + e)

  val constant_1second = RetryPolicies.constantDelay[IO](1.seconds)
  val constant_1hour   = RetryPolicies.constantDelay[IO](1.hour)

}
