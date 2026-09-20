package com.github.chenharryhua.nanjin.terminals

import cats.data.Reader
import cats.effect.Resource
import cats.effect.kernel.Sync
import cats.syntax.option.given
import cats.syntax.eq.given
import fs2.{Chunk, Stream}
import io.circe.Json
import io.circe.jawn.CirceSupportParser.facade
import io.lemonlabs.uri.Url
import kantan.csv.engine.ReaderEngine
import kantan.csv.{CsvConfiguration, CsvReader, ReadResult}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.io.{Decoder, DecoderFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodecFactory}
import org.apache.parquet.hadoop.ParquetReader
import org.typelevel.jawn.AsyncParser
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import squants.information.Information

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IteratorHasAsScala

/** Internal collection of blocking Hadoop readers, each exposing a source file at `url` as an fs2 `Stream` of
  * decoded records.
  *
  * Every reader here follows the same shape:
  *   - all file I/O is wrapped in `F.blocking`, so the underlying blocking Hadoop/JVM calls are shifted onto
  *     the blocking pool rather than running on a compute thread;
  *   - the file handle (and any decompressor) is acquired through `Resource`/`Stream.bracket`, so it is
  *     released even if the stream is cancelled or fails;
  *   - records are emitted in chunks of up to `chunkSize` (or, for `byteS`, a byte buffer sized by the
  *     caller) so downstream sees fs2 chunks aligned to the requested size rather than one element at a time.
  *
  * `url` is resolved to a Hadoop `Path` via `toHadoopPath` (which rewrites the `s3` scheme to `s3a`).
  * Compression is detected from the path extension by Hadoop's `CompressionCodecFactory`, so a compressed
  * file is decompressed transparently; the exception is `parquetS`, which reads through Parquet's own reader
  * and does not go through the shared input-stream path.
  *
  * The `S` suffix marks these as `Stream`-valued (as opposed to the `Resource`-valued `R` builders in
  * `HadoopWriter`). This object is `private`; callers reach it through the higher-level terminal APIs.
  */
private object HadoopReader {

  /** Read a Parquet file as a stream of Avro `GenericData.Record`.
    *
    * `readBuilder` turns the resolved `Path` into a configured `ParquetReader.Builder` (schema projection,
    * filters, Hadoop `Configuration`, etc.), so the caller controls how the file is read. The reader is
    * pulled in chunks: `go` calls `reader.read()` up to `chunkSize` times, stopping early on the `null` that
    * marks end-of-file, and signals completion by returning `None`. Unlike the other readers this does not go
    * through `inputStreamS`, so codec detection is handled by Parquet itself.
    */
  def parquetS[F[_]](
    readBuilder: Reader[Path, ParquetReader.Builder[GenericData.Record]],
    url: Url,
    chunkSize: ChunkSize)(using F: Sync[F]): Stream[F, GenericData.Record] =
    Stream
      .bracket(F.blocking[ParquetReader[GenericData.Record]](readBuilder.run(toHadoopPath(url)).build()))(r =>
        F.blocking(r.close()))
      .flatMap { reader =>
        def go(): (Chunk[GenericData.Record], Option[Unit]) = {
          var counter: Int = 0 // scalafix:ok
          var keepGoing: Boolean = true // scalafix:ok
          val builder = Vector.newBuilder[GenericData.Record]

          while (keepGoing && (counter < chunkSize.value)) { // scalafix:ok
            val gr: GenericData.Record = reader.read()
            if (gr eq null) {
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

  /*
   * input stream based
   */

  /** Open `url` as an `InputStream`, transparently decompressing when the path names a codec.
    *
    * The raw file stream is acquired with `Resource.fromAutoCloseable` so it is always closed. If
    * `CompressionCodecFactory` recognizes the path extension, a pooled `Decompressor` is borrowed for the
    * lifetime of the stream and returned to `CodecPool` on release (the pool reuse is why the decompressor is
    * bracketed separately rather than left to the codec), and the returned stream is the decompressing
    * wrapper. Otherwise the raw stream is returned unchanged.
    */
  private def inputStreamR[F[_]](configuration: Configuration, url: Url)(using
    F: Sync[F]): Resource[F, InputStream] = {
    val path = toHadoopPath(url)
    Resource.fromAutoCloseable(F.blocking(path.getFileSystem(configuration).open(path))).flatMap { is =>
      Option(new CompressionCodecFactory(configuration).getCodec(path)) match {
        case Some(cc) =>
          Resource
            .make(F.blocking(CodecPool.getDecompressor(cc))) { dc =>
              F.blocking(CodecPool.returnDecompressor(dc))
            }
            .map(dc => cc.createInputStream(is, dc))
        case None => Resource.pure(is)
      }
    }
  }

  /** `inputStreamR` lifted into a single-element `Stream`, the shared entry point for every
    * input-stream-based reader below.
    */
  private def inputStreamS[F[_]](configuration: Configuration, url: Url)(using
    F: Sync[F]): Stream[F, InputStream] = Stream.resource[F, InputStream](inputStreamR(configuration, url))

  /** Read an Avro object-container file (embedded schema) as a stream of `GenericData.Record`.
    *
    * `DataFileStream` reads the writer schema from the file header; `readerSchema`, when supplied, drives
    * Avro schema resolution so records are decoded against the reader's expected shape (evolution), otherwise
    * the writer schema is used as-is. The stream is chunked at `chunkSize`. `DataFileStream` holds no
    * resources beyond the `InputStream`, which `inputStreamS` already closes, so it needs no separate
    * bracket.
    */
  def avroS[F[_]](configuration: Configuration, url: Url, chunkSize: ChunkSize, readerSchema: Option[Schema])(
    using F: Sync[F]): Stream[F, GenericData.Record] =
    inputStreamS[F](configuration, url).flatMap { is =>
      val dfs = new DataFileStream[GenericData.Record](
        is,
        readerSchema match {
          case Some(schema) => new GenericDatumReader(null, schema)
          case None         => new GenericDatumReader()
        })
      Stream.fromBlockingIterator[F](dfs.iterator().asScala, chunkSize.value)
    }

  /** Read the raw (post-decompression) bytes of `url`, emitting fixed-size chunks of `bs` bytes.
    *
    * `go` fills a buffer of `bs.toBytes` with repeated `read`s, accumulating across short reads until the
    * buffer is full — emitting a full `bufferSize` chunk and looping — or `read` returns `-1`, emitting the
    * partial final chunk and stopping. This keeps every emitted chunk exactly `bs` bytes except the last,
    * which is why it is documented as respecting the requested chunk size.
    */
  def byteS[F[_]](configuration: Configuration, url: Url, bs: Information)(using
    F: Sync[F]): Stream[F, Byte] =
    inputStreamS[F](configuration, url).flatMap { (is: InputStream) =>
      val bufferSize: Int = bs.toBytes.toInt

      @tailrec
      def go(offset: Int, buffer: Array[Byte]): (Chunk[Byte], Option[Int]) = {
        val numBytes = is.read(buffer, offset, bufferSize - offset)
        if (numBytes === -1) (Chunk.array(buffer, 0, offset), None)
        else if ((numBytes + offset) === bufferSize) (Chunk.array(buffer), 0.some)
        else go(offset + numBytes, buffer)
      }

      Stream.unfoldChunkLoopEval[F, Int, Byte](0)(offset =>
        F.blocking(go(offset, Array.ofDim[Byte](bufferSize))))
    }

  /** Read a stream of JSON values as circe `Json`, chunked at `chunkSize`.
    *
    * Bytes are pulled in 128 KiB reads and fed to a jawn `AsyncParser` in `ValueStream` mode, which
    * incrementally emits complete top-level values as they are parsed (values may be whitespace- or
    * newline-separated; they need not be a single array). `go` accumulates parsed values across reads until
    * it has at least `chunkSize`, then splits off exactly `chunkSize` and carries the remainder into the next
    * chunk; on end-of-input `parser.finish()` flushes any trailing value. A parse error is thrown from within
    * the blocking step so it surfaces as a stream failure.
    */
  def circeS[F[_]](configuration: Configuration, url: Url, chunkSize: ChunkSize)(using
    F: Sync[F]): Stream[F, Json] =
    inputStreamS[F](configuration, url).flatMap { (is: InputStream) =>
      val bufferSize: Int = 131072
      val buffer: Array[Byte] = Array.ofDim[Byte](bufferSize)
      val parser: AsyncParser[Json] = AsyncParser[Json](AsyncParser.ValueStream)
      @tailrec
      def go(existing: Chunk[Json], existCount: Int): (Chunk[Json], Option[Chunk[Json]]) = {
        val numBytes = is.read(buffer, 0, bufferSize)
        if (numBytes === -1) {
          parser.finish() match {
            case Left(ex)     => throw ex // scalafix:ok
            case Right(value) => (existing ++ Chunk.from(value), None)
          }
        } else {
          parser.absorb(ByteBuffer.wrap(buffer, 0, numBytes)) match {
            case Left(ex)     => throw ex // scalafix:ok
            case Right(value) =>
              val size = value.size
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

  /** Read `url` as UTF-8 text, one element per line, chunked at `chunkSize`.
    *
    * Lines are produced by `BufferedReader.lines()`, so the line terminator is stripped and not included in
    * the emitted strings.
    */
  def stringS[F[_]](configuration: Configuration, url: Url, chunkSize: ChunkSize)(using
    F: Sync[F]): Stream[F, String] =
    inputStreamS[F](configuration, url).flatMap { is =>
      val reader = new InputStreamReader(is, StandardCharsets.UTF_8)
      val buffered = new BufferedReader(reader)
      val iterator = buffered.lines().iterator().asScala
      Stream.fromBlockingIterator[F](iterator, chunkSize.value)
    }

  /** Read a CSV file as a stream of rows (each row a `Seq[String]`), chunked at `chunkSize`.
    *
    * Parsing is driven by kantan's CSV engine using `csvConfiguration` (separator, quoting, header). When the
    * configuration declares a header, the first row is dropped so only data rows are emitted. Each row is a
    * `ReadResult`; `rethrow` turns a row-level parse failure into a stream error.
    */
  def kantanS[F[_]](
    configuration: Configuration,
    url: Url,
    chunkSize: ChunkSize,
    csvConfiguration: CsvConfiguration)(using F: Sync[F]): Stream[F, Seq[String]] =
    inputStreamS[F](configuration, url).flatMap { is =>
      val cr: CsvReader[ReadResult[Seq[String]]] =
        ReaderEngine.internalCsvReaderEngine.readerFor(new InputStreamReader(is), csvConfiguration)
      val reader = if (csvConfiguration.hasHeader) cr.drop(1) else cr
      Stream.fromBlockingIterator[F](reader.iterator, chunkSize.value).rethrow
    }

  /*
   * generic record
   */

  /** Shared engine for the schema-driven Avro readers (`jacksonS` and `binAvroS`).
    *
    * A `GenericDatumReader` is built with both `writerSchema` (how the bytes were encoded) and `readerSchema`
    * (the desired shape), so Avro performs schema resolution during decode. The concrete wire format is
    * supplied by `get_decoder`, which wraps the `InputStream` in either a JSON or a binary Avro `Decoder`.
    *
    * `go` reads up to `chunkSize` records per step. Because a raw Avro decoder has no framing or record
    * count, end-of-file is detected by catching the `EOFException` thrown by `datumReader.read`, at which
    * point the records gathered so far are emitted and the stream completes (`None`).
    */
  private def genericRecordReaderS[F[_]](
    get_decoder: InputStream => Decoder,
    configuration: Configuration,
    writerSchema: Schema,
    readerSchema: Schema,
    url: Url,
    chunkSize: ChunkSize)(using F: Sync[F]): Stream[F, GenericData.Record] =
    inputStreamS[F](configuration, url).flatMap { is =>
      val datumReader: GenericDatumReader[GenericData.Record] =
        new GenericDatumReader[GenericData.Record](writerSchema, readerSchema)
      val decoder: Decoder = get_decoder(is)

      def go(): (Chunk[GenericData.Record], Option[Unit]) = {
        val builder = Vector.newBuilder[GenericData.Record]
        var counter: Int = 0 // scalafix:ok
        try {
          while (counter < chunkSize.value) { // scalafix:ok
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

  /** Read Avro records encoded as Avro JSON (the Jackson/`jsonEncoder` form, one schema-shaped JSON document
    * per record) using `genericRecordReaderS` with a JSON `Decoder` built from `writerSchema`.
    */
  def jacksonS[F[_]](
    configuration: Configuration,
    writerSchema: Schema,
    readerSchema: Schema,
    url: Url,
    chunkSize: ChunkSize)(using F: Sync[F]): Stream[F, GenericData.Record] =
    genericRecordReaderS[F](
      get_decoder = (is: InputStream) => DecoderFactory.get.jsonDecoder(writerSchema, is),
      configuration = configuration,
      writerSchema = writerSchema,
      readerSchema = readerSchema,
      url = url,
      chunkSize = chunkSize
    )

  /** Read raw binary Avro records (no object-container header, unlike `avroS`) using `genericRecordReaderS`
    * with a binary `Decoder`. The `null` argument lets Avro allocate a fresh `BinaryDecoder` rather than
    * reusing one.
    */
  def binAvroS[F[_]](
    configuration: Configuration,
    writerSchema: Schema,
    readerSchema: Schema,
    url: Url,
    chunkSize: ChunkSize)(using F: Sync[F]): Stream[F, GenericData.Record] =
    genericRecordReaderS[F](
      get_decoder = (is: InputStream) => DecoderFactory.get.binaryDecoder(is, null),
      configuration = configuration,
      writerSchema = writerSchema,
      readerSchema = readerSchema,
      url = url,
      chunkSize = chunkSize
    )

  /*
   * protobuf
   */

  /** Read length-delimited Protobuf messages of type `A`, chunked at `chunkSize`.
    *
    * The ScalaPB companion `gmc` reads the delimited framing (each message prefixed by its length) via
    * `streamFromDelimitedInput`, matching what a delimited-Protobuf writer produces.
    */
  def protobufS[F[_]: Sync, A <: GeneratedMessage](
    configuration: Configuration,
    url: Url,
    chunkSize: ChunkSize)(using gmc: GeneratedMessageCompanion[A]): Stream[F, A] =
    inputStreamS[F](configuration, url).flatMap { is =>
      Stream.fromBlockingIterator[F](gmc.streamFromDelimitedInput(is).iterator, chunkSize.value)
    }

}
