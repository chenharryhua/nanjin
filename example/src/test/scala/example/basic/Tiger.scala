package example.basic

import cats.effect.IO
import cats.implicits.catsSyntaxEq
import com.github.chenharryhua.nanjin.guard.action.NJMeter
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.terminals.NJPath
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

final case class Tiger(a: Long, c: String)

abstract class WriteRead(agent: Agent[IO]) {
  final protected def write(job: String)(action: NJMeter[IO] => IO[NJPath]): IO[NJPath] = {
    val name = "(write)" + job
    agent
      .gauge(name)
      .timed
      .flatMap(_ => agent.meterR(name, StandardUnit.COUNT))
      .use(meter => agent.action(name, _.notice).retry(action(meter)).logOutput(_.asJson).run)
  }

  final protected def read(job: String)(action: NJMeter[IO] => IO[Long]): IO[Long] = {
    val name = "(read)" + job
    agent
      .gauge(name)
      .timed
      .flatMap(_ => agent.meterR(name, StandardUnit.COUNT))
      .use(meter => agent.action(name, _.notice).retry(action(meter)).logOutput(_.asJson).run)
      .map(_.ensuring(_ === size))
  }
  def single: IO[List[Long]]
  def rotate: IO[List[Long]]
  def sparkSingle: IO[List[Long]]
  def sparkRotate: IO[List[Long]]
  def sparkMulti: IO[List[Long]]

  final def run: IO[Unit] = (single >> rotate >> sparkSingle >> sparkRotate >> sparkMulti).void

}
