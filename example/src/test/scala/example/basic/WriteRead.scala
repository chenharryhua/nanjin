package example.basic

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.guard.service.Agent
import io.lemonlabs.uri.Url

abstract class WriteRead(agent: Agent[IO]) {
  final protected def write(job: Url): Resource[IO, Kleisli[IO, Long, Unit]] = {
    val name = "(write)" + job.toString()
    agent.facilitate(name)(_.meter(name, _.withUnit(_.COUNT)).map(_.kleisli))
  }

  final protected def read(job: Url): Resource[IO, Kleisli[IO, Long, Unit]] = {
    val name = "(read)" + job.toString()
    agent.facilitate(name)(_.meter(name, _.withUnit(_.COUNT)).map(_.kleisli))
  }

  def single: IO[List[Long]]
  def rotate: IO[List[Long]]
  def sparkSingle: IO[List[Long]]
  def sparkRotate: IO[List[Long]]
  def sparkMulti: IO[List[Long]]

  final def run: IO[Unit] = (single >> rotate >> sparkSingle >> sparkRotate >> sparkMulti).void

}
