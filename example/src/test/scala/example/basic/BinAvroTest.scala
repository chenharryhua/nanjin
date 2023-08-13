package example.basic

import cats.effect.IO
import cats.implicits.catsSyntaxEq
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.action.NJMeter
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.terminals.{BinAvroFile, NJCompression, NJPath}
import eu.timepit.refined.auto.*
import example.hadoop
import io.circe.syntax.EncoderOps
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

class BinAvroTest(agent: Agent[IO], base: NJPath) {
  private val root = base / "bin_avro"

  private val files: List[BinAvroFile] = List(
    BinAvroFile(NJCompression.Uncompressed),
    BinAvroFile(NJCompression.Gzip),
    BinAvroFile(NJCompression.Deflate(2)),
    BinAvroFile(NJCompression.Bzip2),
    BinAvroFile(NJCompression.Snappy),
    BinAvroFile(NJCompression.Lz4)
  )

  private def write(job: String)(action: NJMeter[IO] => IO[NJPath]): IO[NJPath] = {
    val name = "(write)" + job
    agent
      .gauge(name)
      .timed
      .flatMap(_ => agent.meterR(name, StandardUnit.COUNT))
      .use(meter => agent.action(name, _.notice).retry(action(meter)).run)
  }

  private val bin_avro = hadoop.binAvro(schema)

  private def writeSingle(file: BinAvroFile): IO[NJPath] = {
    val path = root / "single" / file.fileName
    val sink = bin_avro.sink(path)
    write(path.uri.getPath) { meter =>
      data.evalTap(_ => meter.mark(1)).map(encoder.to).chunkN(1000).through(sink).compile.drain.as(path)
    }
  }

  private def writeRotate(file: BinAvroFile): IO[NJPath] = {
    val path = root / "rotate" / file.fileName
    val sink = bin_avro.sink(policy)(t => path / file.fileName(t))
    write(path.uri.getPath) { meter =>
      data.evalTap(_ => meter.mark(1)).map(encoder.to).chunkN(1000).through(sink).compile.drain.as(path)
    }
  }

  private def writeSingleSpark(file: BinAvroFile): IO[NJPath] = {
    val path = root / "spark" / "single" / file.fileName
    val sink = bin_avro.sink(path)
    write(path.uri.getPath) { meter =>
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

  private def writeMultiSpark(file: BinAvroFile): IO[NJPath] = {
    val path = root / "spark" / "multi" / file.fileName
    write(path.uri.getPath)(_ => table.output.binAvro(path).withCompression(file.compression).run.as(path))
  }

  private def read(job: String)(action: NJMeter[IO] => IO[Long]): IO[Long] = {
    val name = "(read)" + job
    agent
      .gauge(name)
      .timed
      .flatMap(_ => agent.meterR(name, StandardUnit.COUNT))
      .use(meter => agent.action(name, _.notice).retry(action(meter)).logOutput(_.asJson).run)
      .map(_.ensuring(_ === size))
  }

  private def sparkRead(path: NJPath): IO[Long] = {
    read(path.uri.getPath)(_ => loader.binAvro(path).count)
  }

  private def folderRead(path: NJPath): IO[Long] = {
    read(path.uri.getPath) { meter =>
      hadoop
        .filesIn(path)
        .flatMap(bin_avro.source(_).map(decoder.from).evalTap(_ => meter.mark(1)).compile.fold(0L) {
          case (s, _) => s + 1
        })
    }
  }

  private def singleRead(path: NJPath): IO[Long] = {
    read(path.uri.getPath) { meter =>
      bin_avro.source(path).map(decoder.from).evalTap(_ => meter.mark(1)).compile.fold(0L) { case (s, _) =>
        s + 1
      }
    }
  }

  def run: IO[Unit] =
    files.parTraverse(writeSingle).flatMap(ps => ps.parTraverse(singleRead) >> ps.traverse(sparkRead)) >>
      files.parTraverse(writeRotate).flatMap(ps => ps.parTraverse(folderRead) >> ps.traverse(sparkRead)) >>
      files
        .parTraverse(writeSingleSpark)
        .flatMap(ps => ps.parTraverse(singleRead) >> ps.traverse(sparkRead)) >>
      files.traverse(writeMultiSpark).flatMap(ps => ps.parTraverse(folderRead) >> ps.traverse(sparkRead)).void
}
