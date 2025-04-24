package com.github.chenharryhua.nanjin.terminals

import cats.data.Reader
import cats.effect.Resource
import cats.effect.kernel.Sync
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.{Chunk, Pull, Stream}
import io.circe.Json
import io.circe.jawn.CirceSupportParser.facade
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
import squants.information.{Bytes, Information}

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.IteratorHasAsScala

private object HadoopReader {

  def avroS[F[_]](configuration: Configuration, path: Path, chunkSize: ChunkSize)(implicit
    F: Sync[F]): Stream[F, GenericData.Record] =
    for {
      is <- Stream.bracket(F.blocking(HadoopInputFile.fromPath(path, configuration).newStream()))(r =>
        F.blocking(r.close()))
      dfs <- Stream.bracket {
        F.blocking[DataFileStream[GenericData.Record]](new DataFileStream(is, new GenericDatumReader()))
      }(r => F.blocking(r.close()))
      gr <- Stream.fromBlockingIterator[F](dfs.iterator().asScala, chunkSize.value)
    } yield gr

  def parquetS[F[_]](
    readBuilder: Reader[Path, ParquetReader.Builder[GenericData.Record]],
    path: Path,
    chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, GenericData.Record] =
    Stream
      .bracket(F.blocking[ParquetReader[GenericData.Record]](readBuilder.run(path).build()))(r =>
        F.blocking(r.close()))
      .flatMap { reader =>
        val iterator = Iterator.continually(Option(reader.read())).takeWhile(_.nonEmpty).map(_.get)
        Stream.fromBlockingIterator[F](iterator, chunkSize.value)
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
    inputStreamS[F](configuration, path).flatMap { is =>
      val bufferSize: Int     = bs.toBytes.toInt
      val buffer: Array[Byte] = Array.ofDim[Byte](bufferSize)
      def go(offset: Int): Pull[F, Byte, Unit] =
        Pull.eval(F.blocking(is.read(buffer, offset, bufferSize - offset))).flatMap { numBytes =>
          if (numBytes == -1) Pull.output(Chunk.array(buffer, 0, offset)) >> Pull.done
          else if (numBytes + offset == bufferSize) Pull.output(Chunk.array(buffer)) >> go(0)
          else go(offset + numBytes)
        }
      go(0).stream
    }

  def jawnS[F[_]](configuration: Configuration, path: Path, chunkSize: ChunkSize)(implicit
    F: Sync[F]): Stream[F, Json] =
    Stream
      .emit(AsyncParser[Json](AsyncParser.ValueStream))
      .flatMap(parser =>
        byteS[F](configuration, path, Bytes(131072))
          .mapChunks(bs => parser.absorb(bs.toByteBuffer).traverse(Chunk.from))
          .rethrow)
      .chunkN(chunkSize.value)
      .unchunks

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
      val decoder: Decoder = getDecoder(is)
      def go(datumReader: GenericDatumReader[GenericData.Record]): Pull[F, GenericData.Record, Unit] = {
        val builder      = Vector.newBuilder[GenericData.Record]
        var counter: Int = 0
        try {
          while (counter < chunkSize.value) {
            builder += datumReader.read(null, decoder)
            counter += 1
          }
          Pull.output(Chunk.from(builder.result())) >> go(datumReader)
        } catch {
          case _: java.io.EOFException =>
            Pull.output(Chunk.from(builder.result())) >> Pull.done
        }
      }
      go(new GenericDatumReader[GenericData.Record](schema)).stream
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
