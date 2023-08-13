package example.basic


import cats.effect.IO
import cats.implicits.catsSyntaxEq
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.action.NJMeter
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.terminals.{JacksonFile, NJCompression, NJPath}
import eu.timepit.refined.auto.*
import example.hadoop
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

class JacksonTest(agent: Agent[IO], base: NJPath) {
  private val root = base / "jackson"

  private val files: List[JacksonFile] = List(
    JacksonFile(NJCompression.Uncompressed),
    JacksonFile(NJCompression.Gzip),
    JacksonFile(NJCompression.Deflate(2)),
    JacksonFile(NJCompression.Bzip2),
    JacksonFile(NJCompression.Snappy),
    JacksonFile(NJCompression.Lz4)
  )

  private def runAction(name: String)(action: NJMeter[IO] => IO[NJPath]): IO[NJPath] =
    agent
      .gauge(name)
      .timed
      .flatMap(_ => agent.meterR(name, StandardUnit.COUNT))
      .use(meter => agent.action(name, _.notice).retry(action(meter)).run)

  private val jackson = hadoop.jackson(schema)

  private def writeSingle(file: JacksonFile): IO[NJPath] = {
    val name = "write-single-" + file.fileName
    val path = root / "single" / file.fileName
    val sink = jackson. sink(path)
    runAction(name) { meter =>
      data.evalTap(_ => meter.mark(1)).map(encoder.to).chunkN(1000).through(sink).compile.drain.as(path)
    }
  }

  private def writeRotate(file: JacksonFile): IO[NJPath] = {
    val name = "write-rotate-" + file.fileName
    val path = root / "rotate" / file.fileName
    val sink = jackson. sink(policy)(t => path / file.fileName(t))
    runAction(name) { meter =>
      data.evalTap(_ => meter.mark(1)).map(encoder.to).chunkN(1000).through(sink).compile.drain.as(path)
    }
  }

  private def writeSingleSpark(file: JacksonFile): IO[NJPath] = {
    val name = "write-spark-single-" + file.fileName
    val path = root / "spark" / "single" / file.fileName
    val sink = jackson. sink(path)
    runAction(name) { meter =>
      table.output
        .stream(1000)
        .evalTap(_ => meter.mark(1))
        .map(encoder.to)
        .chunks
        .through(sink)
        .compile
        .drain
        .as(path)
    }
  }

  private def writeMultiSpark(file: JacksonFile): IO[NJPath] = {
    val name = "write-spark-multi-" + file.fileName
    val path = root / "spark" / "multi" / file.fileName
    runAction(name)(_ => table.output.jackson(path).withCompression(file.compression).run.as(path))
  }

  private def sparkRead(path: NJPath): IO[Long] =
    agent
      .action("spark-read-" + path.uri.getPath, _.notice)
      .retry(loader.jackson(path).count.map(_.ensuring(_ === size)))
      .logOutput(_.asJson)
      .run

  private def folderRead(path: NJPath): IO[Long] = agent
    .action("folder-read-" + path.uri.getPath, _.notice)
    .retry(
      hadoop
        .filesIn(path)
        .flatMap(jackson.source(_).map(decoder.from).compile.fold(0L) { case (s, _) => s + 1 })
        .map(_.ensuring(_ === size)))
    .logOutput(_.asJson)
    .run

  private def singleRead(path: NJPath): IO[Long] =
    agent
      .action("single-read-" + path.uri.getPath, _.notice)
      .retry(
        jackson
          .source(path)
          .map(decoder.from)
          .compile
          .fold(0L) { case (s, _) => s + 1 }
          .map(_.ensuring(_ === size)))
      .logOutput(_.asJson)
      .run

  def run: IO[Unit] =
    files.parTraverse(writeSingle).flatMap(ps => ps.parTraverse(singleRead) >> ps.traverse(sparkRead)) >>
      files.parTraverse(writeRotate).flatMap(ps => ps.parTraverse(folderRead) >> ps.traverse(sparkRead)) >>
      files
        .parTraverse(writeSingleSpark)
        .flatMap(ps => ps.parTraverse(singleRead) >> ps.traverse(sparkRead)) >>
      files.traverse(writeMultiSpark).flatMap(ps => ps.parTraverse(folderRead) >> ps.traverse(sparkRead)).void
}

