package com.github.chenharryhua.nanjin.terminals

import cats.data.Reader
import cats.effect.Resource
import cats.effect.kernel.Sync
import cats.implicits.{catsSyntaxEq, catsSyntaxOptionId}
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.{Chunk, Stream}
import io.circe.Json
import io.circe.jawn.CirceSupportParser.facade
import kantan.csv.engine.ReaderEngine
import kantan.csv.{CsvConfiguration, CsvReader, ReadResult}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.io.{Decoder, DecoderFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodecFactory, Decompressor}
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.SeekableInputStream
import org.typelevel.jawn.AsyncParser
import squants.information.Information

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IteratorHasAsScala

private object HadoopReader {

  def avroS[F[_]](configuration: Configuration, path: Path, chunkSize: ChunkSize)(implicit
    F: Sync[F]): Stream[F, GenericData.Record] =
    Stream
      .bracket(F.blocking {
        val is = HadoopInputFile.fromPath(path, configuration).newStream()
        new DataFileStream[GenericData.Record](is, new GenericDatumReader())
      })(r => F.blocking(r.close()))
      .flatMap(dfs => Stream.fromBlockingIterator[F](dfs.iterator().asScala, chunkSize.value))

  def parquetS[F[_]](
    readBuilder: Reader[Path, ParquetReader.Builder[GenericData.Record]],
    path: Path,
    chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    Stream
      .bracket(F.blocking[ParquetReader[GenericData.Record]](readBuilder.run(path).build()))(r =>
        F.blocking(r.close()))
      .flatMap { reader =>
        def go(): (Chunk[GenericData.Record], Option[Unit]) = {
          var counter: Int       = 0
          var keepGoing: Boolean = true
          val builder            = Vector.newBuilder[GenericData.Record]
          while (keepGoing && (counter < chunkSize.value)) {
            val gr: GenericData.Record = reader.read()
            if (null == gr) {
              keepGoing = false
            } else {
              builder += gr
              counter += 1
            }
          }

          (Chunk.from(builder.result()), if (keepGoing) Some(()) else None)
        }

        Stream.unfoldChunkLoopEval[F, Unit, GenericData.Record](())(_ => F.blocking(go()))
      }

  private def fileInputStream(path: Path, configuration: Configuration): InputStream = {
    val sis: SeekableInputStream = HadoopInputFile.fromPath(path, configuration).newStream()
    Option(new CompressionCodecFactory(configuration).getCodec(path)) match {
      case Some(cc) =>
        val decompressor: Decompressor = CodecPool.getDecompressor(cc)
        cc.createInputStream(sis, decompressor)
      case None => sis
    }
  }

  def inputStreamR[F[_]](configuration: Configuration, path: Path)(implicit
    F: Sync[F]): Resource[F, InputStream] =
    Resource.make(F.blocking(fileInputStream(path, configuration)))(r => F.blocking(r.close()))

  def inputStreamS[F[_]](configuration: Configuration, path: Path)(implicit
    F: Sync[F]): Stream[F, InputStream] = Stream.resource(inputStreamR(configuration, path))

  // respect chunk size
  def byteS[F[_]](configuration: Configuration, path: Path, bs: Information)(implicit
    F: Sync[F]): Stream[F, Byte] =
    inputStreamS[F](configuration, path).flatMap { (is: InputStream) =>
      val bufferSize: Int     = bs.toBytes.toInt
      val buffer: Array[Byte] = Array.ofDim[Byte](bufferSize)

      @tailrec
      def go(offset: Int): (Chunk[Byte], Option[Int]) = {
        val numBytes = is.read(buffer, offset, bufferSize - offset)
        if (numBytes == -1) (Chunk.array(buffer, 0, offset), None)
        else if ((numBytes + offset) === bufferSize) (Chunk.array(buffer), 0.some)
        else go(offset + numBytes)
      }

      Stream.unfoldChunkLoopEval[F, Int, Byte](0)(offset => F.blocking(go(offset)))
    }

  def jawnS[F[_]](configuration: Configuration, path: Path, chunkSize: ChunkSize)(implicit
    F: Sync[F]): Stream[F, Json] =
    inputStreamS[F](configuration, path).flatMap { (is: InputStream) =>
      val bufferSize: Int           = 131072
      val buffer: Array[Byte]       = Array.ofDim[Byte](bufferSize)
      val parser: AsyncParser[Json] = AsyncParser[Json](AsyncParser.ValueStream)
      @tailrec
      def go(existing: Chunk[Json], existCount: Int): (Chunk[Json], Option[Chunk[Json]]) = {
        val numBytes = is.read(buffer, 0, bufferSize)
        if (numBytes == -1) {
          parser.finish() match {
            case Left(ex)     => throw ex
            case Right(value) => (existing ++ Chunk.from(value), None)
          }
        } else {
          parser.absorb(ByteBuffer.wrap(buffer, 0, numBytes)) match {
            case Left(ex)     => throw ex
            case Right(value) =>
              val size  = value.size
              val jsons = Chunk.from(value)
              if ((existCount + size) < chunkSize.value)
                go(existing ++ jsons, existCount + size)
              else {
                val (first, second) = jsons.splitAt(chunkSize.value - existCount)
                (existing ++ first, second.some)
              }
          }
        }
      }

      Stream.unfoldChunkLoopEval[F, Chunk[Json], Json](Chunk.empty)(ck => F.blocking(go(ck, ck.size)))
    }

  def stringS[F[_]](configuration: Configuration, path: Path, chunkSize: ChunkSize)(implicit
    F: Sync[F]): Stream[F, String] =
    inputStreamS[F](configuration, path).flatMap { is =>
      val reader   = new InputStreamReader(is, StandardCharsets.UTF_8)
      val buffered = new BufferedReader(reader)
      val iterator = buffered.lines().iterator().asScala
      Stream.fromBlockingIterator[F](iterator, chunkSize.value)
    }

  def kantanS[F[_]](
    configuration: Configuration,
    path: Path,
    chunkSize: ChunkSize,
    csvConfiguration: CsvConfiguration)(implicit F: Sync[F]): Stream[F, Seq[String]] =
    Stream
      .bracket(F.blocking {
        val cr: CsvReader[ReadResult[Seq[String]]] = ReaderEngine.internalCsvReaderEngine
          .readerFor(new InputStreamReader(fileInputStream(path, configuration)), csvConfiguration)
        if (csvConfiguration.hasHeader) cr.drop(1) else cr
      })(r => F.blocking(r.close()))
      .flatMap(reader => Stream.fromBlockingIterator[F](reader.iterator, chunkSize.value).rethrow)

  private def genericRecordReaderS[F[_]](
    getDecoder: InputStream => Decoder,
    configuration: Configuration,
    schema: Schema,
    path: Path,
    chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    inputStreamS[F](configuration, path).flatMap { is =>
      val datumReader: GenericDatumReader[GenericData.Record] =
        new GenericDatumReader[GenericData.Record](schema)
      val decoder: Decoder = getDecoder(is)

      def go(): (Chunk[GenericData.Record], Option[Unit]) = {
        val builder      = Vector.newBuilder[GenericData.Record]
        var counter: Int = 0
        try {
          while (counter < chunkSize.value) {
            builder += datumReader.read(null, decoder)
            counter += 1
          }
          (Chunk.from(builder.result()), ().some)
        } catch {
          case _: java.io.EOFException =>
            (Chunk.from(builder.result()), None)
        }
      }

      Stream.unfoldChunkLoopEval[F, Unit, GenericData.Record](())(_ => F.blocking(go()))
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
