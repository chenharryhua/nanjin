package example.basic

import cats.effect.IO
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.zones.{cairoTime, saltaTime}
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.terminals.{JacksonFile, NJCompression}
import eu.timepit.refined.auto.*
import example.hadoop
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
class JacksonTest(agent: Agent[IO], base: Url) extends WriteRead(agent) {
  private val root = base / "jackson"

  private val files: List[JacksonFile] = List(
    JacksonFile(NJCompression.Uncompressed),
    JacksonFile(NJCompression.Gzip),
    JacksonFile(NJCompression.Deflate(2)),
    JacksonFile(NJCompression.Bzip2),
    JacksonFile(NJCompression.Snappy),
    JacksonFile(NJCompression.Lz4)
  )

  private val jackson = hadoop.jackson(schema)

  private def writeSingle(file: JacksonFile): IO[Url] = {
    val path = root / "single" / file.fileName
    val sink = jackson.sink(path)
    write(path).use { meter =>
      data.evalTap(_ => meter.update(1)).map(encoder.to).chunks.through(sink).compile.drain.as(path)
    }
  }

  private def writeRotate(file: JacksonFile): IO[Url] = {
    val path = root / "rotate" / file.fileName
    val sink = jackson.sink(policy, cairoTime)(t => path / file.fileName(t))
    write(path).use { meter =>
      data.evalTap(_ => meter.update(1)).map(encoder.to).chunks.through(sink).compile.drain.as(path)
    }
  }

  private def writeSingleSpark(file: JacksonFile): IO[Url] = {
    val path = root / "spark" / "single" / file.fileName
    val sink = jackson.sink(path)
    write(path).use { meter =>
      table
        .stream[IO](1000)
        .evalTap(_ => meter.update(1))
        .map(encoder.to)
        .chunks
        .through(sink)
        .compile
        .drain
        .as(path)
    }
  }

  private def writeMultiSpark(file: JacksonFile): IO[Url] = {
    val path = root / "spark" / "multi" / file.fileName
    write(path).use(_ =>
      table.output.jackson(path).withCompression(file.compression).run[IO].as(path))
  }

  private def writeRotateSpark(file: JacksonFile): IO[Url] = {
    val path = root / "spark" / "rotate" / file.fileName
    val sink = jackson.sink(policy, saltaTime)(t => path / file.fileName(t))
    write(path).use { meter =>
      table
        .stream[IO](1000)
        .evalTap(_ => meter.update(1))
        .map(encoder.to)
        .chunks
        .through(sink)
        .compile
        .drain
        .as(path)
    }
  }

  private def sparkRead(path: Url): IO[Long] =
    read(path).use(_ => loader.jackson(path).count[IO])

  private def folderRead(path: Url): IO[Long] =
    read(path).use { meter =>
      hadoop
        .filesIn(path)
        .flatMap(
          _.traverse(jackson.source(_, 100).map(decoder.from).evalTap(_ => meter.update(1)).compile.fold(0L) {
            case (s, _) => s + 1
          }))
        .map(_.sum)
    }

  private def singleRead(path: Url): IO[Long] =
    read(path).use { meter =>
      jackson.source(path, 100).map(decoder.from).evalTap(_ => meter.update(1)).compile.fold(0L) {
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
