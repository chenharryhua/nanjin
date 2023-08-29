package example.basic

import cats.effect.IO
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.terminals.{KantanFile, NJCompression, NJPath}
import eu.timepit.refined.auto.*
import example.hadoop
import kantan.csv.generic.*
import kantan.csv.{CsvConfiguration, RowDecoder, RowEncoder}

class KantanTest(agent: Agent[IO], base: NJPath, rfc: CsvConfiguration) extends WriteRead(agent) {
  private val header = if (rfc.hasHeader) "kantan-with-header" else "kantan-without-header"
  private val root   = base / header

  private val files: List[KantanFile] = List(
    KantanFile(NJCompression.Uncompressed),
    KantanFile(NJCompression.Deflate(2)),
    KantanFile(NJCompression.Bzip2),
    KantanFile(NJCompression.Snappy),
    KantanFile(NJCompression.Gzip),
    KantanFile(NJCompression.Lz4)
  )

  private val kantan = hadoop.kantan(rfc)

  implicit private val rowEncoder: RowEncoder[Tiger] = shapeless.cachedImplicit
  implicit private val rowDecoder: RowDecoder[Tiger] = shapeless.cachedImplicit

  private def writeSingle(file: KantanFile): IO[NJPath] = {
    val path = root / "single" / file.fileName
    val sink = kantan.sink(path)
    write(path.uri.getPath) { meter =>
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
    val path = root / "rotate" / file.fileName
    val sink = kantan.sink(policy)(t => path / file.fileName(t))
    write(path.uri.getPath) { meter =>
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
    val path = root / "spark" / "single" / file.fileName
    val sink = kantan.sink(path)
    write(path.uri.getPath) { meter =>
      table.output
        .stream(1000)
        .evalTap(_ => meter.mark(1))
        .map(rowEncoder.encode)
        .chunkN(1000)
        .through(sink)
        .compile
        .drain
        .as(path)
    }
  }

  private def writeMultiSpark(file: KantanFile): IO[NJPath] = {
    val path = root / "spark" / "multi" / file.fileName
    write(path.uri.getPath)(_ =>
      table.output.kantan(path, rfc).withCompression(file.compression).run.as(path))
  }

  private def writeRotateSpark(file: KantanFile): IO[NJPath] = {
    val path = root / "spark" / "rotate" / file.fileName
    val sink = kantan.sink(policy)(t => path / file.fileName(t))
    write(path.uri.getPath) { meter =>
      table.output
        .stream(1000)
        .evalTap(_ => meter.mark(1))
        .map(rowEncoder.encode)
        .chunkN(1000)
        .through(sink)
        .compile
        .drain
        .as(path)
    }
  }

  private def sparkRead(path: NJPath): IO[Long] =
    read(path.uri.getPath)(_ => loader.kantan(path, rfc).count)

  private def folderRead(path: NJPath): IO[Long] =
    read(path.uri.getPath) { meter =>
      hadoop
        .filesIn(path)
        .flatMap(
          kantan.source(_, 1000).map(rowDecoder.decode).rethrow.evalTap(_ => meter.mark(1)).compile.fold(0L) {
            case (s, _) => s + 1
          })
    }

  private def singleRead(path: NJPath): IO[Long] =
    read(path.uri.getPath) { meter =>
      kantan.source(path, 1000).map(rowDecoder.decode).rethrow.evalTap(_ => meter.mark(1)).compile.fold(0L) {
        case (s, _) =>
          s + 1
      }
    }

  val single: IO[List[Long]] =
    files.parTraverse(writeSingle).flatMap(ps => ps.parTraverse(singleRead) >> ps.traverse(sparkRead))

  val rotate: IO[List[Long]] =
    files.parTraverse(writeRotate).flatMap(ps => ps.parTraverse(folderRead) >> ps.traverse(sparkRead))

  val sparkSingle: IO[List[Long]] =
    files.parTraverse(writeSingleSpark).flatMap(ps => ps.parTraverse(singleRead) >> ps.traverse(sparkRead))

  val sparkRotate: IO[List[Long]] =
    files.parTraverse(writeRotateSpark).flatMap(ps => ps.parTraverse(folderRead) >> ps.traverse(sparkRead))

  val sparkMulti: IO[List[Long]] =
    files.traverse(writeMultiSpark).flatMap(ps => ps.parTraverse(folderRead) >> ps.traverse(sparkRead))

}
