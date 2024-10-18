package example.basic

import cats.effect.IO
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.zones.{darwinTime, utcTime}
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.terminals.{KantanFile, NJCompression}
import eu.timepit.refined.auto.*
import example.hadoop
import kantan.csv.{CsvConfiguration, RowDecoder, RowEncoder}
import kantan.csv.generic.*
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
class KantanTest(agent: Agent[IO], base: Url, rfc: CsvConfiguration) extends WriteRead(agent) {
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

  private def writeSingle(file: KantanFile): IO[Url] = {
    val path = root / "single" / file.fileName
    val sink = kantan.sink(path)
    write(path).use { meter =>
      data.evalTap(_ => meter.update(1)).map(rowEncoder.encode).chunks.through(sink).compile.drain.as(path)
    }
  }

  private def writeRotate(file: KantanFile): IO[Url] = {
    val path = root / "rotate" / file.fileName
    val sink = kantan.sink(policy, darwinTime)(t => path / file.fileName(t))
    write(path).use { meter =>
      data.evalTap(_ => meter.update(1)).map(rowEncoder.encode).chunks.through(sink).compile.drain.as(path)
    }
  }

  private def writeSingleSpark(file: KantanFile): IO[Url] = {
    val path = root / "spark" / "single" / file.fileName
    val sink = kantan.sink(path)
    write(path).use { meter =>
      table
        .stream[IO](1000)
        .evalTap(_ => meter.update(1))
        .map(rowEncoder.encode)
        .chunks
        .through(sink)
        .compile
        .drain
        .as(path)
    }
  }

  private def writeMultiSpark(file: KantanFile): IO[Url] = {
    val path = root / "spark" / "multi" / file.fileName
    write(path).use(_ =>
      table.output.kantan(path, rfc).withCompression(file.compression).run[IO].as(path))
  }

  private def writeRotateSpark(file: KantanFile): IO[Url] = {
    val path = root / "spark" / "rotate" / file.fileName
    val sink = kantan.sink(policy, utcTime)(t => path / file.fileName(t))
    write(path).use { meter =>
      table
        .stream[IO](1000)
        .evalTap(_ => meter.update(1))
        .map(rowEncoder.encode)
        .chunks
        .through(sink)
        .compile
        .drain
        .as(path)
    }
  }

  private def sparkRead(path: Url): IO[Long] =
    read(path).use(_ => loader.kantan(path, rfc).count[IO])

  private def folderRead(path: Url): IO[Long] =
    read(path).use { meter =>
      hadoop
        .filesIn(path)
        .flatMap(
          _.traverse(
            kantan
              .source(_, 1000)
              .map(rowDecoder.decode)
              .rethrow
              .evalTap(_ => meter.update(1))
              .compile
              .fold(0L) { case (s, _) =>
                s + 1
              }))
        .map(_.sum)
    }

  private def singleRead(path: Url): IO[Long] =
    read(path).use { meter =>
      kantan
        .source(path, 1000)
        .mapChunks(_.map(rowDecoder.decode))
        .rethrow
        .evalTap(_ => meter.update(1))
        .compile
        .fold(0L) { case (s, _) =>
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
