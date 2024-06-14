package com.github.chenharryhua.nanjin.terminals

import cats.data.Reader
import cats.effect.Resource
import cats.effect.kernel.Sync
import cats.implicits.{catsSyntaxOptionId, toFlatMapOps, toFunctorOps}
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.{Chunk, Stream}
import io.circe.Json
import io.circe.jawn.CirceSupportParser.facade
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.io.{Decoder, DecoderFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.SeekableInputStream
import org.typelevel.jawn.AsyncParser
import squants.information.Information

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.IteratorHasAsScala

private object HadoopReader {

  def avroR[F[_]](configuration: Configuration, schema: Schema, path: Path)(implicit
    F: Sync[F]): Resource[F, DataFileStream[GenericData.Record]] =
    for {
      is <- Resource.make(F.blocking(HadoopInputFile.fromPath(path, configuration).newStream()))(r =>
        F.blocking(r.close()))
      dfs <- Resource.make[F, DataFileStream[GenericData.Record]] {
        F.blocking[DataFileStream[GenericData.Record]](new DataFileStream(is, new GenericDatumReader(schema)))
      }(r => F.blocking(r.close()))
    } yield dfs

  def parquetR[F[_]](readBuilder: Reader[Path, ParquetReader.Builder[GenericData.Record]], path: Path)(
    implicit F: Sync[F]): Resource[F, ParquetReader[GenericData.Record]] =
    Resource.make {
      F.blocking[ParquetReader[GenericData.Record]](readBuilder.run(path).build())
    }(r => F.blocking(r.close()))

  private def fileInputStream(path: Path, configuration: Configuration): InputStream = {
    val sis: SeekableInputStream = HadoopInputFile.fromPath(path, configuration).newStream()
    Option(new CompressionCodecFactory(configuration).getCodec(path)) match {
      case Some(cc) => cc.createInputStream(sis)
      case None     => sis
    }
  }

  def inputStreamR[F[_]](configuration: Configuration, path: Path)(implicit
    F: Sync[F]): Resource[F, InputStream] =
    Resource.make(F.blocking(fileInputStream(path, configuration)))(r => F.blocking(r.close()))

  def inputStreamS[F[_]](configuration: Configuration, path: Path)(implicit
    F: Sync[F]): Stream[F, InputStream] = Stream.resource(inputStreamR(configuration, path))

  // best effort
  def byteS[F[_]](configuration: Configuration, path: Path, bufferSize: Information)(implicit
    F: Sync[F]): Stream[F, Byte] =
    inputStreamS[F](configuration, path).flatMap { is =>
      val size: Int           = bufferSize.toBytes.toInt
      val buffer: Array[Byte] = Array.ofDim[Byte](size)

      Stream.unfoldChunkEval[F, Unit, Byte](()) { _ =>
        F.blocking(is.read(buffer, 0, size)).map { numBytes =>
          if (numBytes == -1)
            None
          else if (numBytes == size)
            Some((Chunk.array(buffer), ()))
          else
            Some((Chunk.array(buffer, 0, numBytes), ()))
        }
      }
    }

  // respect chunk size
  def byteS[F[_]](configuration: Configuration, path: Path, chunkSize: ChunkSize)(implicit
    F: Sync[F]): Stream[F, Byte] =
    inputStreamS[F](configuration, path).flatMap { is =>
      val size: Int           = chunkSize.value
      val buffer: Array[Byte] = Array.ofDim[Byte](size)

      Stream.unfoldLoopEval[F, Int, Chunk[Byte]](0) { offset =>
        F.blocking(is.read(buffer, offset, size - offset)).map { numBytes =>
          if (numBytes == -1) // churn buffer
            (Chunk.array(buffer, 0, offset), None)
          else if (numBytes + offset == size)
            (Chunk.array(buffer), Some(0))
          else
            (Chunk.empty, Some(offset + numBytes))
        }
      }
    }.unchunks

  def jawnS[F[_]](configuration: Configuration, path: Path, bufferSize: Information)(implicit
    F: Sync[F]): Stream[F, Json] =
    inputStreamS[F](configuration, path).flatMap { is =>
      type Status = (AsyncParser[Json], Int)
      val size: Int           = bufferSize.toBytes.toInt
      val buffer: Array[Byte] = Array.ofDim[Byte](size)
      // parser and offset
      val initStatus: Status = (AsyncParser[Json](AsyncParser.ValueStream), 0)

      Stream.unfoldLoopEval[F, Status, Chunk[Json]](initStatus) { case (parser, offset) =>
        F.blocking(is.read(buffer, offset, size - offset)).flatMap { numBytes =>
          if (numBytes == -1)
            F.fromEither(parser.finalAbsorb(buffer.slice(0, offset))).map(js => (Chunk.from(js), None))
          else if (numBytes + offset == size)
            F.fromEither(parser.absorb(buffer)).map(js => (Chunk.from(js), (parser, 0).some))
          else
            F.pure((Chunk.empty[Json], (parser, numBytes + offset).some))
        }
      }
    }.unchunks

  def stringS[F[_]](configuration: Configuration, path: Path, chunkSize: ChunkSize)(implicit
    F: Sync[F]): Stream[F, String] =
    inputStreamS[F](configuration, path).flatMap { is =>
      val iterator: Iterator[String] =
        new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)).lines().iterator().asScala
      Stream.fromBlockingIterator[F](iterator, chunkSize.value)
    }

  private def genericRecordReaderS[F[_]](
    getDecoder: InputStream => Decoder,
    configuration: Configuration,
    schema: Schema,
    path: Path,
    chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    inputStreamS[F](configuration, path).flatMap { is =>
      val iterator: Iterator[GenericData.Record] = {
        val decoder: Decoder = getDecoder(is)
        val datumReader: GenericDatumReader[GenericData.Record] =
          new GenericDatumReader[GenericData.Record](schema)

        Iterator.continually {
          try {
            val gr: GenericData.Record = datumReader.read(null, decoder)
            Some(gr)
          } catch {
            case _: java.io.EOFException => None
          }
        }.takeWhile(_.nonEmpty).map(_.get) // faster than .flatten
      }

      Stream.fromBlockingIterator[F](iterator, chunkSize.value)
    }

  def jacksonS[F[_]](configuration: Configuration, schema: Schema, path: Path, chunkSize: ChunkSize)(implicit
    F: Sync[F]): Stream[F, GenericData.Record] =
    genericRecordReaderS[F](
      getDecoder = (is: InputStream) => DecoderFactory.get.jsonDecoder(schema, is),
      configuration = configuration,
      schema = schema,
      path = path,
      chunkSize = chunkSize)

  def binAvroS[F[_]](configuration: Configuration, schema: Schema, path: Path, chunkSize: ChunkSize)(implicit
    F: Sync[F]): Stream[F, GenericData.Record] =
    genericRecordReaderS[F](
      getDecoder = (is: InputStream) => DecoderFactory.get.binaryDecoder(is, null),
      configuration = configuration,
      schema = schema,
      path = path,
      chunkSize = chunkSize)
}
