package example.basic

import cats.effect.IO
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.zones.{mumbaiTime, singaporeTime}
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.terminals.{BinAvroFile, NJCompression}
import eu.timepit.refined.auto.*
import example.hadoop
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*

class BinAvroTest(agent: Agent[IO], base: Url) extends WriteRead(agent) {
  private val root = base / "bin_avro"

  private val files: List[BinAvroFile] = List(
    BinAvroFile(NJCompression.Uncompressed),
    BinAvroFile(NJCompression.Gzip),
    BinAvroFile(NJCompression.Deflate(2)),
    BinAvroFile(NJCompression.Bzip2),
    BinAvroFile(NJCompression.Snappy),
    BinAvroFile(NJCompression.Lz4)
  )

  private val bin_avro = hadoop.binAvro(schema)

  private def writeSingle(file: BinAvroFile): IO[Url] = {
    val path = root / "single" / file.fileName
    val sink = bin_avro.sink(path)
    write(path).use { meter =>
      data.evalTap(_ => meter.update(1)).map(encoder.to).chunks.through(sink).compile.drain.as(path)
    }
  }

  private def writeRotate(file: BinAvroFile): IO[Url] = {
    val path = root / "rotate" / file.fileName
    val sink = bin_avro.sink(policy, mumbaiTime)(t => path / file.fileName(t))
    write(path).use { meter =>
      data.evalTap(_ => meter.update(1)).map(encoder.to).chunks.through(sink).compile.drain.as(path)
    }
  }

  private def writeSingleSpark(file: BinAvroFile): IO[Url] = {
    val path = root / "spark" / "single" / file.fileName
    val sink = bin_avro.sink(path)
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

  private def writeRotateSpark(file: BinAvroFile): IO[Url] = {
    val path = root / "spark" / "rotate" / file.fileName
    val sink = bin_avro.sink(policy, singaporeTime)(t => path / file.fileName(t))
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

  private def writeMultiSpark(file: BinAvroFile): IO[Url] = {
    val path = root / "spark" / "multi" / file.fileName
    write(path).use(_ => table.output.binAvro(path).withCompression(file.compression).run[IO].as(path))
  }

  private def sparkRead(path: Url): IO[Long] =
    read(path).use(_ => loader.binAvro(path).count[IO])

  private def folderRead(path: Url): IO[Long] =
    read(path).use { meter =>
      hadoop
        .filesIn(path)
        .flatMap(_.traverse(
          bin_avro.source(_, 100).map(decoder.from).evalTap(_ => meter.update(1)).compile.fold(0L) {
            case (s, _) => s + 1
          }))
        .map(_.sum)
    }

  private def singleRead(path: Url): IO[Long] =
    read(path).use { meter =>
      bin_avro.source(path, 100).map(decoder.from).evalTap(_ => meter.update(1)).compile.fold(0L) {
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
