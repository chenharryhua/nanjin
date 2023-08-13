package example.basic

import cats.effect.IO
import cats.implicits.catsSyntaxEq
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.action.NJMeter
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.terminals.{KantanFile, NJCompression, NJPath}
import eu.timepit.refined.auto.*
import example.hadoop
import io.circe.syntax.EncoderOps
import kantan.csv.generic.*
import kantan.csv.{CsvConfiguration, RowDecoder, RowEncoder}
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

class KantanTest(agent: Agent[IO], base: NJPath, rfc: CsvConfiguration) {
  private val root = base / "kantan_no_header"

  private val files: List[KantanFile] = List(
    KantanFile(NJCompression.Uncompressed),
    KantanFile(NJCompression.Deflate(2)),
    KantanFile(NJCompression.Bzip2),
    KantanFile(NJCompression.Snappy),
    KantanFile(NJCompression.Gzip),
    KantanFile(NJCompression.Lz4)
  )

  private def runAction(name: String)(action: NJMeter[IO] => IO[NJPath]): IO[NJPath] =
    agent
      .gauge(name)
      .timed
      .flatMap(_ => agent.meterR(name, StandardUnit.COUNT))
      .use(meter => agent.action(name, _.notice).retry(action(meter)).run)

  private val kantan = hadoop.kantan(rfc)

  implicit private val rowEncoder: RowEncoder[Tiger] = shapeless.cachedImplicit
  implicit private val rowDecoder: RowDecoder[Tiger] = shapeless.cachedImplicit

  private def writeSingle(file: KantanFile): IO[NJPath] = {
    val name = "write-single-" + file.fileName
    val path = root / "single" / file.fileName
    val sink = kantan.sink(path)
    runAction(name) { meter =>
      data
        .evalTap(_ => meter.mark(1))
        .map(rowEncoder.encode)
        .chunkN(1000)
        .through(sink)
        .compile
        .drain
        .as(path)
    }
  }

  private def writeRotate(file: KantanFile): IO[NJPath] = {
    val name = "write-rotate-" + file.fileName
    val path = root / "rotate" / file.fileName
    val sink = kantan.sink(policy)(t => path / file.fileName(t))
    runAction(name) { meter =>
      data
        .evalTap(_ => meter.mark(1))
        .map(rowEncoder.encode)
        .chunkN(1000)
        .through(sink)
        .compile
        .drain
        .as(path)
    }
  }

  private def writeSingleSpark(file: KantanFile): IO[NJPath] = {
    val name = "write-spark-single-" + file.fileName
    val path = root / "spark" / "single" / file.fileName
    val sink = kantan.sink(path)
    runAction(name) { meter =>
      table.output
        .stream(1000)
        .evalTap(_ => meter.mark(1))
        .map(rowEncoder.encode)
        .chunks
        .through(sink)
        .compile
        .drain
        .as(path)
    }
  }

  private def writeMultiSpark(file: KantanFile): IO[NJPath] = {
    val name = "write-spark-multi-" + file.fileName
    val path = root / "spark" / "multi" / file.fileName
    runAction(name)(_ => table.output.kantan(path, rfc).withCompression(file.compression).run.as(path))
  }

  private def sparkRead(path: NJPath): IO[Long] =
    agent
      .action("spark-read-" + path.uri.getPath, _.notice)
      .retry(loader.kantan(path, rfc).count.map(_.ensuring(_ === size)))
      .logOutput(_.asJson)
      .run

  private def folderRead(path: NJPath): IO[Long] = agent
    .action("folder-read-" + path.uri.getPath, _.notice)
    .retry(
      hadoop
        .filesIn(path)
        .flatMap(kantan.source(_, 1000).map(rowDecoder.decode).rethrow.compile.fold(0L) { case (s, _) =>
          s + 1
        })
        .map(_.ensuring(_ === size)))
    .logOutput(_.asJson)
    .run

  private def singleRead(path: NJPath): IO[Long] =
    agent
      .action("single-read-" + path.uri.getPath, _.notice)
      .retry(
        kantan
          .source(path, 1000)
          .map(rowDecoder.decode)
          .rethrow
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
