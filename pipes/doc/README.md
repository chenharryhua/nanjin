# Pipes and file terminals

The Pipes module provides two related APIs:

- `com.github.chenharryhua.nanjin.pipes` contains in-memory fs2 transformations for raw binary Avro
  and Avro's Jackson JSON encoding.
- `com.github.chenharryhua.nanjin.terminals` reads, writes, rotates, lists, moves, and retains files
  through Hadoop-compatible filesystems.

Supported terminal formats include Avro object containers, raw binary Avro, Avro JSON, Parquet,
Kantan CSV, raw bytes, Circe JSON, Jackson `JsonNode`, text, and length-delimited Protobuf.

## Contents

- [Dependency](#dependency)
- [In-memory Avro pipes](#in-memory-avro-pipes)
  - [Binary Avro](#binary-avro)
  - [Avro JSON](#avro-json)
- [Hadoop filesystem](#hadoop-filesystem)
  - [Create a terminal](#create-a-terminal)
  - [Filesystem operations](#filesystem-operations)
- [File sources and sinks](#file-sources-and-sinks)
  - [Write and read Avro](#write-and-read-avro)
  - [Copy between formats](#copy-between-formats)
  - [Format reference](#format-reference)
  - [Chunking and lifecycle](#chunking-and-lifecycle)
- [Compression and filenames](#compression-and-filenames)
  - [Compression matrix](#compression-matrix)
  - [Filename rules](#filename-rules)
- [Rotating sinks](#rotating-sinks)
  - [Rotate by size](#rotate-by-size)
  - [Rotate by policy](#rotate-by-policy)
  - [Rotation guarantees and edge cases](#rotation-guarantees-and-edge-cases)
- [Partition paths and retention](#partition-paths-and-retention)
- [Wire format](#wire-format)
- [Resource and operational guidance](#resource-and-operational-guidance)
- [Source map and validation](#source-map-and-validation)

## Dependency

Add the module:

```scala
libraryDependencies += "com.github.chenharryhua" %% "nj-pipes" % "<version>"
```

The module includes Hadoop client APIs. Access to S3A, HDFS, or another filesystem still requires the
matching Hadoop implementation, credentials, endpoint settings, and filesystem configuration in the
application environment.

## In-memory Avro pipes

The two `nanjin.pipes` objects transform `Stream[F, GenericRecord]` to and from `Stream[F, Byte]`.
They do not open files and do not add an Avro object-container header.

### Binary Avro

`binaryAvro.toBytes` writes consecutive raw Avro records. `binaryAvro.fromBytes` reads until EOF using
the supplied schema:

```scala
import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.binaryAvro
import com.sksamuel.avro4s.{FromRecord, SchemaFor, ToRecord}
import fs2.Stream
import io.circe.Codec

final case class Event(id: Long, message: String) derives Codec.AsObject

val schema = SchemaFor[Event].schema
val toRecord = ToRecord[Event](schema)
val fromRecord = FromRecord[Event](schema)
val events = List(Event(1L, "started"), Event(2L, "finished"))

val binaryRoundTrip: IO[List[Event]] =
  Stream
    .emits(events)
    .covary[IO]
    .map(toRecord.to)
    .through(binaryAvro.toBytes[IO](schema))
    .through(binaryAvro.fromBytes[IO](schema))
    .map(fromRecord.from)
    .compile
    .toList
```

The raw byte stream carries no embedded writer schema or record count. The reader must receive the
correct writer schema out of band. Use an Avro object-container file when the file must carry its own
schema and block framing.

### Avro JSON

`jackson.toBytes` and `jackson.fromBytes` use Avro's `JsonEncoder` and JSON decoder rather than a
normal Jackson `ObjectMapper`:

```scala
import com.github.chenharryhua.nanjin.pipes.jackson

val jsonRoundTrip: IO[List[Event]] =
  Stream
    .emits(events)
    .covary[IO]
    .map(toRecord.to)
    .through(jackson.toBytes[IO](schema))
    .through(jackson.fromBytes[IO](schema))
    .map(fromRecord.from)
    .compile
    .toList
```

The Avro JSON pipe uses ISO-8859-1 bytes on both sides because that is the charset used by Avro's JSON
encoder in this path. It inserts the platform line separator between upstream chunks. This differs
from the terminal text, Circe, and CSV writers, which use UTF-8.

## Hadoop filesystem

### Create a terminal

Construct `Hadoop[F]` with an `org.apache.hadoop.conf.Configuration`. Paths use
`io.lemonlabs.uri.Url`:

```scala
import cats.effect.IO
import com.github.chenharryhua.nanjin.terminals.Hadoop
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import org.apache.hadoop.conf.Configuration

val configuration = new Configuration()
val hadoop = Hadoop[IO](configuration)
val root = Url.parse("./data/events")
val path = root / "plain.txt"
```

Relative paths use the configured local filesystem. HDFS and object-store URLs are resolved through
Hadoop. An `s3://` URL is normalized to `s3a://`; ensure the S3A implementation and credentials are
available.

### Filesystem operations

| API | Behavior |
| --- | --- |
| `exists(path)` | Tests whether a file or directory exists |
| `locatedFileStatus(path)` | Recursively lists files |
| `dataFolders(path)` | Returns folders that directly contain discovered files |
| `emptyFolders(path)` | Recursively finds empty directories |
| `filesIn(path)` | Lists direct, non-hidden files ordered by modification time; a file path returns itself |
| `filesIn(path, filter)` | Uses a custom Hadoop `PathFilter` |
| `copy(source, target)` | Copies and preserves the source |
| `move(source, target)` | Copies and removes the source after success |
| `delete(path)` | Recursively deletes the path |

Missing paths produce empty lists for listing operations. `copy` and `move` overwrite the target
through Hadoop `FileUtil.copy`. On object stores, copy and move are not atomic; an interrupted move
can leave objects at both locations. `delete` is recursive and irreversible.

## File sources and sinks

`hadoop.source(url)` returns lazy read streams. `hadoop.sink(url)` returns write pipes. A sink creates
or overwrites its target when the pipe runs and emits one `Int` per input chunk, representing the
number of records or bytes written in that chunk.

### Write and read Avro

An Avro object-container file embeds its writer schema. The sink derives the schema from the first
record; the source can use the embedded schema or resolve it against a supplied reader schema:

```scala
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.terminals.{AvroFile, AvroCompression}

val avroFile = AvroFile(_.Snappy)
val avroPath = root / avroFile.fileName

val writeAvro: IO[Long] =
  Stream
    .emits(events)
    .covary[IO]
    .map(toRecord.to)
    .through(hadoop.sink(avroPath).avro(AvroCompression.Snappy))
    .compile
    .fold(0L)(_ + _)

val readAvro: IO[List[Event]] =
  hadoop
    .source(avroPath)
    .avro(chunkSize = 128, readerSchema = schema)
    .map(fromRecord.from)
    .compile
    .toList

val writeThenRead: IO[List[Event]] = writeAvro >> readAvro
```

Every record sent to a schema-derived `GenericRecord` sink must be compatible with the first record's
schema. An empty input creates no file because no schema can be derived. Policy-based rotating sinks
also offer explicit-schema overloads when a file must be opened before the first record arrives.

### Copy between formats

Because sources and sinks are fs2 streams and pipes, format conversion is direct. This example reads
newline-delimited Circe JSON and writes compact text lines:

```scala
import io.circe.Json

val jsonPath = root / "circe.json.gz"
val textPath = root / "plain.txt.gz"

val copyJsonToText: IO[Long] =
  hadoop
    .source(jsonPath)
    .circe(chunkSize = 256)
    .map((json: Json) => json.noSpaces)
    .through(hadoop.sink(textPath).text)
    .compile
    .fold(0L)(_ + _)
```

The `.gz` suffix makes Hadoop wrap the non-Avro, non-Parquet source and sink in its configured Gzip
codec. The sink count is per input element, not the number of compressed bytes.

### Format reference

| Source/sink method | Element type | Encoding and schema behavior |
| --- | --- | --- |
| `avro` | `GenericRecord` | Avro object-container file with embedded schema; optional reader schema on read |
| `binAvro` | `GenericRecord` | Bare binary Avro records; writer schema is required by the reader |
| `jackson` | `GenericRecord` | Avro JSON encoding; writer schema is required by the reader |
| `parquet` | `GenericRecord` | Avro-backed Parquet; builder hooks configure reader and writer |
| `kantan` | `Seq[String]` | Kantan CSV using RFC defaults or a custom `CsvConfiguration` |
| `bytes` | `Byte` | Raw bytes; the reader takes a Squants byte-buffer size |
| `inputStream` | `InputStream` | Sink only; consumes and closes each supplied stream |
| `circe` | `Json` | Compact UTF-8 JSON values; writer emits one value per line |
| `jsonNode` | `JsonNode` | Jackson writer/reader supplied by the caller; one value per non-empty line |
| `text` | `String` | UTF-8 lines; writer appends a platform line separator |
| `protobuf` | ScalaPB `GeneratedMessage` | Length-delimited messages; source requires the generated companion |

For `binAvro` and `jackson`, separate writer and reader schemas enable Avro schema resolution. The
single-schema overload uses one schema for both sides. A CSV configuration with a header writes that
header once and the corresponding reader drops it.

`CsvHeaderOf[A]` derives an explicit CSV header from a case class's field names and can transform each
name with `modify`.

### Chunking and lifecycle

All source `chunkSize` arguments count decoded records, lines, rows, or messages and must be greater
than zero. `bytes` instead accepts a Squants `Information` buffer size of at least one byte. Chunk size
controls streaming granularity; it does not limit the total file size.

Files are opened only when a source or sink stream runs. Hadoop and JVM handles, decompression
resources, readers, writers, and input streams are closed on normal completion, failure, or
cancellation. Reads and writes run in `F.blocking`.

Most writers flush after each input chunk. Parquet buffers internally and writes trailing row groups
and its footer when the stream finalizes, so completing or canceling the resource lifecycle is
required for a valid file.

## Compression and filenames

### Compression matrix

| Format | Supported compression choices |
| --- | --- |
| Avro object container | Uncompressed, Snappy, Bzip2, Deflate, Xz, Zstandard |
| Binary Avro | Uncompressed, Snappy, Bzip2, Gzip, Lz4, Lz4Raw, Deflate |
| Jackson Avro JSON | Uncompressed, Snappy, Bzip2, Gzip, Lz4, Lz4Raw, Deflate |
| Circe JSON | Uncompressed, Snappy, Bzip2, Gzip, Lz4, Lz4Raw, Deflate |
| Kantan CSV | Uncompressed, Snappy, Bzip2, Gzip, Lz4, Lz4Raw, Deflate |
| Text | Uncompressed, Snappy, Bzip2, Gzip, Lz4, Lz4Raw, Deflate |
| Protobuf | Uncompressed, Snappy, Bzip2, Gzip, Lz4, Deflate |
| Parquet | Uncompressed, Snappy, Gzip, Lz4, Lz4Raw, Brotli, Lzo, Zstandard |
| Raw bytes | Any codec recognized by Hadoop's `CompressionCodecFactory` from the path suffix |

`Deflate`, `Xz`, and `Zstandard` accept levels one through nine where their format-specific companion
exposes them. Compression JSON uses short names such as `gzip`, `snappy`, `deflate-5`, `xz-5`, and
`zstd-5`.

For Avro object containers, pass the selected `AvroCompression` to the sink so Avro configures its
internal block codec. For Parquet, pass `_.withCompressionCodec(file.compression.codecName)` to the
writer builder. A `ParquetFile` name describes the intended codec but does not configure the writer
by itself. Other terminal writers and readers detect compression from the filename extension through
Hadoop.

### Filename rules

`FileFormat` has these exact suffixes:

| Format | Suffix |
| --- | --- |
| `Unknown` | `unknown.unknown` |
| `Jackson` | `jackson.json` |
| `Circe` | `circe.json` |
| `Text` | `plain.txt` |
| `Kantan` | `kantan.csv` |
| `Parquet` | `apache.parquet` |
| `Avro` | `data.avro` |
| `BinaryAvro` | `binary.avro` |
| `JavaObject` | `java.obj` |
| `Protobuf` | `google.pb` |

Avro and Parquet put the compression name before the format suffix, for example
`snappy.data.avro` and `zstd.apache.parquet`. Other formats append the compression extension, for
example `circe.json.gz`, `plain.txt.snappy`, and `google.pb.deflate`.

`FileKind` supplies deterministic naming helpers:

- `fileName` returns the format/compression filename.
- `fileName(time)` prefixes `yyyyMMdd-HHmmss`.
- `fileName(create)` prefixes the first five UUID characters, a zero-padded index of at least four
  digits, and `HHmmss`, for example `12345-0007-030405.plain.txt`.
- `ymdFileName(create)` prefixes `Year=yyyy/Month=MM/Day=dd/`.

`FileFormat.JavaObject` participates in `FileFormat` JSON and `Compression.fileName`, but there is no
`FileKind`, `FileSource`, or `FileSink` method for Java object serialization.

## Rotating sinks

A rotating sink uses the same format methods as `FileSink` but emits one `RotateFile` for every
completed output file. The path builder receives `CreateRotateFile`, containing the sequence UUID,
1-based index, and opening time.

### Rotate by size

```scala
import com.github.chenharryhua.nanjin.terminals.CirceFile
import io.circe.syntax.*

import java.time.ZoneId

val rotatedRoot = root / "rotated"
val fileKind = CirceFile(_.Gzip)

val rotateBySize =
  hadoop.rotateSink(ZoneId.of("UTC"), size = 10000) { create =>
    rotatedRoot / fileKind.ymdFileName(create)
  }

val completedFiles =
  Stream
    .emits(events)
    .covary[IO]
    .map(_.asJson)
    .through(rotateBySize.circe)
    .compile
    .toList
```

The size must be positive. Each file contains exactly the configured number of input elements except
the final file, which contains the remainder. A single large upstream chunk is split across as many
files as required.

### Rotate by policy

```scala
import scala.concurrent.duration.*

val rotateByTime =
  hadoop.rotateSink(
    ZoneId.of("UTC"),
    _.fixedDelay(1.minute).repeat
  ) { create =>
    rotatedRoot / fileKind.ymdFileName(create)
  }

val completedByTime =
  Stream
    .emits(events)
    .covary[IO]
    .map(_.asJson)
    .through(rotateByTime.circe)
    .compile
    .toList
```

The first policy tick opens the first file; each later tick closes it and opens the next. Use a policy
that remains active for the input's lifetime. Policy rotation merges data and ticks with
`mergeHaltBoth`, so the sink stops when either stream ends. A finite policy can therefore stop
consuming before a longer input stream finishes.

### Rotation guarantees and edge cases

- Only one writer is open at a time. Opening the next file finalizes the previous writer first.
- The final partially filled file is finalized and emitted when input completes.
- `RotateFile.recordCount` values sum to the number of consumed input elements.
- Size- and policy-based sinks for bytes, JSON, CSV, text, and Protobuf create and report one
  zero-record file for empty input.
- Schema-derived `GenericRecord` rotation creates no file for empty input because no schema is
  available. Explicit-schema policy overloads can open an empty file.
- Schema-derived Avro, binary Avro, Jackson, and Parquet rotation uses the first record's schema and
  assumes all later records are compatible.
- A write failure becomes `RotateWriteException`. Its `offset` is the number of records already
  written to the current file, not a total across the rotation sequence.
- Policy windows with no input may produce zero-record files.

## Partition paths and retention

`partitionPath` formats Hive-style directory names:

```scala
import com.github.chenharryhua.nanjin.terminals.partitionPath

import java.time.{LocalDate, LocalDateTime}

partitionPath.ymd(LocalDate.of(2026, 1, 2))
// Year=2026/Month=01/Day=02

partitionPath.ymdh(LocalDateTime.of(2026, 1, 2, 3, 4))
// Year=2026/Month=01/Day=02/Hour=03

partitionPath.ymdhm(LocalDateTime.of(2026, 1, 2, 3, 4))
// Year=2026/Month=01/Day=02/Hour=03/Minute=04
```

The component parsers validate width and bounds. Field names are case-insensitive when parsing, but
formatting uses `Year`, `Month`, `Day`, `Hour`, and `Minute` exactly. `extractDate` finds a consecutive
year/month/day sequence anywhere in a URL and rejects invalid calendar dates.

`latestYmd`, `earliestYmd`, `latestYmdh`, and `earliestYmdh` traverse one partition level at a time.
`best` exposes the same selection algorithm for custom directory parsers.

Date retention is destructive:

```scala
val retain = hadoop.dateFolderRetention(
  root,
  keeps = List(LocalDate.of(2026, 1, 2), LocalDate.of(2026, 1, 1))
)
```

Folders containing files are marked `Retained`, `Removed`, or `RemoveFailed`. Folders without a
parseable date are retained. The overload taking `startFrom` and `backwardDays` retains dates in
`[startFrom - backwardDays + 1, startFrom]`; a non-positive `backwardDays` retains no dated folders.
Review every `FolderRetentionResult` rather than assuming deletion succeeded.

## Wire format

These values are persisted or externally observed and must remain stable unless a migration is
planned:

- `FileFormat` JSON values are the exact, case-sensitive suffixes in the filename table.
- Compression JSON values are exact short names. Leveled forms are `deflate-<1-9>`, `xz-<1-9>`, and
  `zstd-<1-9>`.
- `RetentionStatus` JSON values are `Removed`, `RemoveFailed`, and `Retained`.
- Derived `FileKind` JSON uses the subtype names `AvroFile`, `CirceFile`, `BinAvroFile`,
  `JacksonFile`, `KantanFile`, `ParquetFile`, `ProtobufFile`, and `TextFile`.
- Filenames and partition directory components follow the rules above.
- Circe and text terminals use UTF-8. Jackson `JsonNode` output is UTF-8 JSON. The CSV writer uses
  UTF-8, while its reader uses the JVM default charset. The in-memory Avro JSON pipe uses ISO-8859-1.
- Protobuf files use length-delimited message framing.
- Raw binary Avro and Avro JSON require the writer schema out of band; Avro object-container and
  Parquet files carry schema metadata.

`RotateFile` JSON uses exactly these keys:

| Key | Meaning |
| --- | --- |
| `index` | 1-based file index within the rotation sequence |
| `url` | Completed file URL |
| `recordCount` | Number of records written to that file |
| `create` | Local opening date-time |
| `closed` | Local closing date-time |
| `window` | Formatted elapsed duration |
| `zoneId` | Zone used to interpret `create` and `closed` |
| `sequenceId` | Rotation sequence UUID |

`window` is write-only in the current codec. The encoder writes it, while the decoder ignores it and
recomputes the duration from `create`, `closed`, and `zoneId`. JSON without `window` still decodes.

Changing a Scala identifier is wire-safe only when it leaves all suffixes, compression names,
filenames, partition components, enum values, subtype tags, framing, and JSON keys unchanged.

## Resource and operational guidance

- Default sink builders overwrite their target. Write to a staging path first when replacement must
  be atomic or recoverable; a custom Parquet builder can select another write mode.
- Keep source and sink streams inside their fs2 lifecycle. Canceling or failing a stream closes the
  underlying handles, but a partially written file can remain.
- Ensure Parquet streams finalize so buffered rows and the footer are written.
- Choose positive chunk sizes and byte buffers. Larger values improve throughput at the cost of
  memory and cancellation latency.
- Match raw binary Avro and Avro JSON readers to the original writer schema. Use a compatible reader
  schema for evolution.
- Ensure every schema-derived `GenericRecord` stream uses one compatible schema.
- Make every rotation path unique. Reusing a path overwrites an earlier file.
- Use an unbounded or sufficiently long-lived policy for policy rotation; a finite policy can truncate
  consumption.
- Treat recursive deletion, retention, and move as destructive. Object-store moves are not atomic.
- Configure and test filesystem codecs in the deployment environment; a filename alone cannot load a
  missing native or Hadoop codec implementation.

## Source map and validation

| Area | Source |
| --- | --- |
| In-memory binary Avro | [`binaryAvro.scala`](../src/main/scala/com/github/chenharryhua/nanjin/pipes/binaryAvro.scala) |
| In-memory Avro JSON | [`jackson.scala`](../src/main/scala/com/github/chenharryhua/nanjin/pipes/jackson.scala) |
| Filesystem entry point | [`Hadoop.scala`](../src/main/scala/com/github/chenharryhua/nanjin/terminals/Hadoop.scala) |
| File sources | [`FileSource.scala`](../src/main/scala/com/github/chenharryhua/nanjin/terminals/FileSource.scala) |
| File sinks | [`FileSink.scala`](../src/main/scala/com/github/chenharryhua/nanjin/terminals/FileSink.scala) |
| Blocking readers and writers | [`HadoopReader.scala`](../src/main/scala/com/github/chenharryhua/nanjin/terminals/HadoopReader.scala), [`HadoopWriter.scala`](../src/main/scala/com/github/chenharryhua/nanjin/terminals/HadoopWriter.scala) |
| Compression and formats | [`Compression.scala`](../src/main/scala/com/github/chenharryhua/nanjin/terminals/Compression.scala), [`FileFormat.scala`](../src/main/scala/com/github/chenharryhua/nanjin/terminals/FileFormat.scala), [`FileKind.scala`](../src/main/scala/com/github/chenharryhua/nanjin/terminals/FileKind.scala) |
| Rotation | [`RotateSink.scala`](../src/main/scala/com/github/chenharryhua/nanjin/terminals/RotateSink.scala), [`RotateFile.scala`](../src/main/scala/com/github/chenharryhua/nanjin/terminals/RotateFile.scala) |
| Partition paths | [`partitionPath.scala`](../src/main/scala/com/github/chenharryhua/nanjin/terminals/partitionPath.scala) |
| Retention results | [`RetentionStatus.scala`](../src/main/scala/com/github/chenharryhua/nanjin/terminals/RetentionStatus.scala) |

From the repository root:

```shell
sbt 'pipes / Test / compile'
sbt 'pipes / test'
```
