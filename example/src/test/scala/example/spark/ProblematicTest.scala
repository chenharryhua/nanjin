package example.spark

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import example.sparKafka
import org.scalatest.funsuite.AnyFunSuite

class ProblematicTest extends AnyFunSuite {
  private val service: ServiceGuard[IO] = TaskGuard[IO]("g").service("s")
  private val path: NJPath              = NJPath("./non.exist.folder")

  test("problematic") {
    assertThrows[Exception](sparKafka.stats.circe(path).summary[IO])
  }

  test("lazy retry") {
    service.eventStream(
      _.action("a").retry(sparKafka.stats.circe(path).summary[IO]).buildWith(identity).use(_.run(())))
  }

  test("lazy batch") {
    service.eventStream(
      _.batch("b").sequential(
        sparKafka.stats.circe(path).summary[IO],
        sparKafka.stats.circe(path).summary[IO],
        sparKafka.stats.circe(path).summary[IO]
      ))

    service.eventStream(
      _.batch("b").parallel(
        sparKafka.stats.circe(path).summary[IO],
        sparKafka.stats.circe(path).summary[IO],
        sparKafka.stats.circe(path).summary[IO]
      ))

    service.eventStream(
      _.batch("b").quasi.sequential(
        sparKafka.stats.circe(path).summary[IO],
        sparKafka.stats.circe(path).summary[IO],
        sparKafka.stats.circe(path).summary[IO]
      ))

    service.eventStream(
      _.batch("b").quasi.parallel(
        sparKafka.stats.circe(path).summary[IO],
        sparKafka.stats.circe(path).summary[IO],
        sparKafka.stats.circe(path).summary[IO]
      ))
  }
}
