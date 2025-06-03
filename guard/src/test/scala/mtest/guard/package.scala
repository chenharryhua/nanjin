package mtest

import cats.effect.IO
import cats.effect.std.Random
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.guard.event.Event
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps

import java.time.ZoneId
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

package object guard {

  final val beijingTime: ZoneId = ZoneId.of("Asia/Shanghai")

  def unit_fun: IO[Unit] = IO(())

  def add_fun(a: Int, b: Int): IO[Int] = IO(a + b)

  val random_error: IO[Unit] =
    Random.scalaUtilRandom[IO].flatMap(_.nextBoolean.ifM(IO(()), IO.raiseError[Unit](new Exception(s"oops"))))

  def never_fun: IO[Int] = IO.never[Int]

  def err_fun(i: Int): IO[Int] = IO.raiseError[Int](new Exception(s"oops: $i"))

  def fun1(i: Int): IO[Long] = IO(i + 1L)
  def fun2(a: Int, b: Int): IO[Int] = IO(a + b)
  def fun3(a: Int, b: Int, c: Int): IO[Int] = IO(a + b + c)
  def fun4(a: Int, b: Int, c: Int, d: Int): IO[Int] = IO(a + b + c + d)
  def fun5(a: Int, b: Int, c: Int, d: Int, e: Int): IO[Int] = IO(a + b + c + d + e)

  def fun0fut: IO[Future[Int]] = IO(Future(1))
  def fun1fut(i: Int): Future[Int] = Future(i + 1)
  def fun2fut(a: Int, b: Int): Future[Int] = Future(a + b)
  def fun3fut(a: Int, b: Int, c: Int): Future[Int] = Future(a + b + c)
  def fun4fut(a: Int, b: Int, c: Int, d: Int): Future[Int] = Future(a + b + c + d)
  def fun5fut(a: Int, b: Int, c: Int, d: Int, e: Int): Future[Int] = Future(a + b + c + d + e)

  val fmt: DurationFormatter = DurationFormatter.defaultFormatter

  def checkJson(evt: Event): Event =
    decode[Event](evt.asJson.noSpaces) match {
      case Left(value)  => throw value
      case Right(value) =>
        assert(value == evt, s"${evt.toString} \n-------- ${value.toString}")
        value
    }
}
