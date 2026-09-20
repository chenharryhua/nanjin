package mtest.terminals

import cats.effect.IO
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.terminals.*
import fs2.Stream
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import munit.CatsEffectSuite
import mtest.terminals.HadoopTestData.*
import org.apache.avro.generic.GenericRecord

import java.time.ZoneId
import scala.concurrent.duration.*

/** Tests for RotateByPolicy schema-aware overloads:
  *   - avro(schema, compression)
  *   - binAvro(schema)
  *   - jackson(schema)
  *   - parquet(schema, f)
  *
  * These overloads allow the schema to be specified upfront rather than inferred from the first record.
  */
class RotateByPolicySchemaTest extends CatsEffectSuite {
  val zoneId: ZoneId = ZoneId.systemDefault()
  val number: Long = 5000L

  private def data: Stream[IO, GenericRecord] =
    Stream.emits(pandaSet.toList).covary[IO].repeatN(number)

  test("1.avro(schema, compression) - policy rotation with explicit schema") {
    val path: Url = Url.parse("./data/test/terminals/rotate-policy-schema/avro")
    val file = AvroFile(_.Snappy)
    val run = for {
      _ <- hdp.delete(path)
      processedSize <- data
        .through(
          hdp
            .rotateSink(zoneId, _.fixedDelay(0.1.second).repeat)(t => path / file.fileName(t))
            .avro(pandaSchema, _.Snappy))
        .fold(0L)((sum, v) => sum + v.recordCount)
        .compile
        .lastOrError
      size <-
        hdp
          .filesIn(path)
          .flatMap(_.traverse(hdp.source(_).avro(100).compile.toList.map(_.size)))
          .map(_.sum)
    } yield {
      assert(size == number * 2)
      assert(processedSize == number * 2)
    }
    run
  }

  test("2.avro(schema) - uncompressed with explicit schema") {
    val path: Url = Url.parse("./data/test/terminals/rotate-policy-schema/avro-uncompressed")
    val file = AvroFile(_.Uncompressed)
    val run = for {
      _ <- hdp.delete(path)
      processedSize <- data
        .through(
          hdp.rotateSink(zoneId, _.fixedDelay(0.1.second).repeat)(t => path / file.fileName(t)).avro(
            pandaSchema))
        .fold(0L)((sum, v) => sum + v.recordCount)
        .compile
        .lastOrError
      size <-
        hdp
          .filesIn(path)
          .flatMap(_.traverse(hdp.source(_).avro(100).compile.toList.map(_.size)))
          .map(_.sum)
    } yield {
      assert(size == number * 2)
      assert(processedSize == number * 2)
    }
    run
  }

  test("3.binAvro(schema) - policy rotation with explicit schema") {
    val path: Url = Url.parse("./data/test/terminals/rotate-policy-schema/bin-avro")
    val file = BinAvroFile(_.Uncompressed)
    val run = for {
      _ <- hdp.delete(path)
      processedSize <- data
        .through(
          hdp
            .rotateSink(zoneId, _.fixedDelay(0.1.second).repeat)(t => path / file.fileName(t))
            .binAvro(pandaSchema))
        .fold(0L)((sum, v) => sum + v.recordCount)
        .compile
        .lastOrError
      size <-
        hdp
          .filesIn(path)
          .flatMap(_.traverse(hdp.source(_).binAvro(100, pandaSchema).compile.toList.map(_.size)))
          .map(_.sum)
    } yield {
      assert(size == number * 2)
      assert(processedSize == number * 2)
    }
    run
  }

  test("4.jackson(schema) - policy rotation with explicit schema") {
    val path: Url = Url.parse("./data/test/terminals/rotate-policy-schema/jackson")
    val file = JacksonFile(_.Uncompressed)
    val run = for {
      _ <- hdp.delete(path)
      processedSize <- data
        .through(
          hdp
            .rotateSink(zoneId, _.fixedDelay(0.1.second).repeat)(t => path / file.fileName(t))
            .jackson(pandaSchema))
        .fold(0L)((sum, v) => sum + v.recordCount)
        .compile
        .lastOrError
      size <-
        hdp
          .filesIn(path)
          .flatMap(_.traverse(hdp.source(_).jackson(100, pandaSchema).compile.toList.map(_.size)))
          .map(_.sum)
    } yield {
      assert(size == number * 2)
      assert(processedSize == number * 2)
    }
    run
  }

  test("5.parquet(schema, f) - policy rotation with explicit schema") {
    val path: Url = Url.parse("./data/test/terminals/rotate-policy-schema/parquet")
    val file = ParquetFile(_.Snappy)
    val run = for {
      _ <- hdp.delete(path)
      processedSize <- data
        .through(
          hdp
            .rotateSink(zoneId, _.fixedDelay(0.1.second).repeat)(t => path / file.fileName(t))
            .parquet(pandaSchema))
        .fold(0L)((sum, v) => sum + v.recordCount)
        .compile
        .lastOrError
      size <-
        hdp
          .filesIn(path)
          .flatMap(_.traverse(hdp.source(_).parquet(100).compile.toList.map(_.size)))
          .map(_.sum)
    } yield {
      assert(size == number * 2)
      assert(processedSize == number * 2)
    }
    run
  }

  test("6.avro(schema) - empty stream still produces file") {
    val path: Url = Url.parse("./data/test/terminals/rotate-policy-schema/avro-empty")
    val file = AvroFile(_.Uncompressed)
    val run = hdp.delete(path) >> Stream.empty
      .covaryAll[IO, GenericRecord]
      .through(
        hdp
          .rotateSink(zoneId, _.fixedDelay(1.second).repeat.limited(2))(t => path / file.fileName(t))
          .avro(pandaSchema))
      .compile
      .toList

    run.map { results =>
      // With explicit schema, files are created even for empty streams
      assert(results.nonEmpty)
      assert(results.forall(_.recordCount == 0))
    }
  }
}
