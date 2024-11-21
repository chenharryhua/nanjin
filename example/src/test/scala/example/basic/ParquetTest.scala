package example.basic

import cats.effect.IO
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.zones.{beijingTime, sydneyTime}
import com.github.chenharryhua.nanjin.guard.service.Agent
import com.github.chenharryhua.nanjin.terminals.{NJCompression, ParquetFile}
import eu.timepit.refined.auto.*
import example.hadoop
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
class ParquetTest(agent: Agent[IO], base: Url) extends WriteRead(agent) {
  private val root = base / "parquet"

  private val files: List[ParquetFile] = List(
    ParquetFile(NJCompression.Uncompressed),
    ParquetFile(NJCompression.Gzip),
    ParquetFile(NJCompression.Snappy),
    ParquetFile(NJCompression.Lz4),
    ParquetFile(NJCompression.Lz4_Raw),
    ParquetFile(NJCompression.Zstandard(3))
  )

  private val parquet = hadoop.parquet(schema)

  private def writeSingle(file: ParquetFile): IO[Url] = {
    val path = root / "single" / file.fileName
    val sink = parquet.updateWriter(_.withCompressionCodec(file.compression.codecName)).sink(path)
    write(path).use { meter =>
      data.evalTap(_ => meter.run(1)).map(encoder.to).chunks.through(sink).compile.drain.as(path)
    }
  }

  private def writeRotate(file: ParquetFile): IO[Url] = {
    val path = root / "rotate" / file.fileName
    val sink = parquet
      .updateWriter(_.withCompressionCodec(file.compression.codecName))
      .rotateSink(policy, beijingTime)(t => path / file.fileName(t))
    write(path).use { meter =>
      data.evalTap(_ => meter.run(1)).map(encoder.to).chunks.through(sink).compile.drain.as(path)
    }
  }

  private def writeSingleSpark(file: ParquetFile): IO[Url] = {
    val path = root / "spark" / "single" / file.fileName
    val sink = parquet.updateWriter(_.withCompressionCodec(file.compression.codecName)).sink(path)
    write(path).use { meter =>
      table
        .stream[IO](1000)
        .evalTap(_ => meter.run(1))
        .map(encoder.to)
        .chunks
        .through(sink)
        .compile
        .drain
        .as(path)
    }
  }

  private def writeMultiSpark(file: ParquetFile): IO[Url] = {
    val path = root / "spark" / "multi" / file.fileName
    write(path).use(_ =>
      table.output.parquet(path).withCompression(file.compression).run[IO].as(path))
  }

  private def writeRotateSpark(file: ParquetFile): IO[Url] = {
    val path = root / "spark" / "rotate" / file.fileName
    val sink = parquet
      .updateWriter(_.withCompressionCodec(file.compression.codecName))
      .rotateSink(policy, sydneyTime)(t => path / file.fileName(t))
    write(path).use { meter =>
      table
        .stream[IO](1000)
        .evalTap(_ => meter.run(1))
        .map(encoder.to)
        .chunks
        .through(sink)
        .compile
        .drain
        .as(path)
    }
  }

  private def sparkRead(path: Url): IO[Long] =
    read(path).use(_ => loader.parquet(path).count[IO])

  private def folderRead(path: Url): IO[Long] =
    read(path).use { meter =>
      hadoop
        .filesIn(path)
        .flatMap(
          _.traverse(parquet.source(_, 100).map(decoder.from).evalTap(_ => meter.run(1)).compile.fold(0L) {
            case (s, _) => s + 1
          }))
        .map(_.sum)
    }

  private def singleRead(path: Url): IO[Long] =
    read(path).use { meter =>
      parquet.source(path, 100).map(decoder.from).evalTap(_ => meter.run(1)).compile.fold(0L) {
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
