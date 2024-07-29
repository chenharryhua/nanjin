package example.basic

import cats.effect.IO
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.guard.action.NJMeter
import com.github.chenharryhua.nanjin.guard.service.Agent

abstract class WriteRead(agent: Agent[IO]) {
  final protected def write(job: String): Resource[IO, NJMeter[IO]] = {
    val name = "(write)" + job
    for {
      _ <- agent.gauge(name).timed
      meter <- agent.meter(name, _.withUnit(_.COUNT))
    } yield meter
  }

  final protected def read(job: String): Resource[IO, NJMeter[IO]] = {
    val name = "(read)" + job
    for {
      _ <- agent.gauge(name).timed
      meter <- agent.meter(name, _.withUnit(_.COUNT))
    } yield meter
  }
  def single: IO[List[Long]]
  def rotate: IO[List[Long]]
  def sparkSingle: IO[List[Long]]
  def sparkRotate: IO[List[Long]]
  def sparkMulti: IO[List[Long]]

  final def run: IO[Unit] = (single >> rotate >> sparkSingle >> sparkRotate >> sparkMulti).void

}
