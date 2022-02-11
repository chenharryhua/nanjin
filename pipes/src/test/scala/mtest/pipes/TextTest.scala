package mtest.pipes

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.TextSerde
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import mtest.terminals.akkaSystem

import scala.concurrent.duration.*
import scala.concurrent.Await

class TextTest extends AnyFunSuite {
  import TestData.*
  val expected: List[String] = tigers.map(_.toString)

  test("fs2 text identity") {
    val data: Stream[IO, String] = Stream.emits(expected)
    assert(data.through(TextSerde.serPipe).through(TextSerde.deserPipe).compile.toList.unsafeRunSync() === expected)
  }

  test("akka text identity") {
    val src: Source[String, NotUsed] = Source(expected)
    implicit val mat: Materializer   = Materializer(akkaSystem)
    val rst = src.via(TextSerde.serFlow).via(TextSerde.deserFlow).runFold(List.empty[String])(_.appended(_))
    assert(Await.result(rst, 1.minute) === expected)
  }
}
