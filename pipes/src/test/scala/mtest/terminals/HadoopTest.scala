package mtest.terminals

import better.files.*
import cats.effect.IO
import com.github.chenharryhua.nanjin.terminals.RetentionStatus.{Removed, Retained}
import com.github.chenharryhua.nanjin.terminals.partitionPath.*
import com.github.chenharryhua.nanjin.terminals.{extractDate, toHadoopPath, FolderRetentionResult}
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import munit.CatsEffectSuite
import mtest.terminals.HadoopTestData.hdp

import java.time.{LocalDate, LocalDateTime}

class HadoopTest extends CatsEffectSuite {
  private val path: Url = Url.parse("./data/test/terminals/hadoop")

  private val p1 = path / ymdh(LocalDateTime.of(2023, 8, 28, 2, 0, 0)) / "a.txt"
  private val p2 = path / ymdh(LocalDateTime.of(2023, 8, 29, 1, 0, 0)) / "b.txt"
  private val p3 = path / ymdh(LocalDateTime.of(2023, 8, 30, 0, 0, 0)) / "c.txt"
  private val p4 = path / ymdh(LocalDateTime.of(2023, 8, 30, 1, 0, 0)) / "d.txt"

  File(p1.toString()).createFileIfNotExists(createParents = true)
  File(p2.toString()).createFileIfNotExists(createParents = true)
  File(p3.toString()).createFileIfNotExists(createParents = true)
  File(p4.toString()).createFileIfNotExists(createParents = true)

  test("1.ymd") {
    for {
      earliest <- hdp.earliestYmd(path)
      latest <- hdp.latestYmd(path)
    } yield {
      assert(earliest.get.toString().takeRight(25) == "Year=2023/Month=08/Day=28")
      assert(latest.get.toString().takeRight(25) == "Year=2023/Month=08/Day=30")
    }
  }
  test("2.ymdh") {
    for {
      earliest <- hdp.earliestYmdh(path)
      latest <- hdp.latestYmdh(path)
    } yield {
      assert(earliest.get.toString().takeRight(33) == "Year=2023/Month=08/Day=28/Hour=02")
      assert(latest.get.toString().takeRight(33) == "Year=2023/Month=08/Day=30/Hour=01")
    }
  }
  test("3.exist") {
    for {
      exists <- hdp.exists(p1)
      statuses <- hdp.locatedFileStatus(path)
    } yield {
      assert(exists)
      assert(statuses.count(_.isFile) >= 4)
    }
  }

  test("4.file in") {
    hdp.filesIn(p1).map { files =>
      assert(files.size == 1)
      assert(files.head.toString().takeRight(5) == "a.txt")
    }
  }

  test("5.delete") {
    val delAction = for {
      before <- hdp.exists(p1)
      del <- hdp.delete(p1)
      after <- hdp.exists(p1)
    } yield (before, del, after)

    delAction.map { case (before, del, after) =>
      assert(before)
      assert(del)
      assert(!after)
    }
  }

  test("6.empty folders") {
    val emptyFolder = path / "empty"
    val nestedEmptyFolder = path / "nested" / "sub"
    val nestedParent = path / "nested" / "parent"
    File(emptyFolder.toString()).createDirectories()
    File(nestedEmptyFolder.toString()).createDirectories()
    File(nestedParent.toString()).createDirectories()

    hdp.emptyFolders(path).map(_.map(_.toString)).map { empties =>
      assert(empties.exists(_.endsWith("/empty")))
      assert(empties.exists(_.endsWith("/sub")))
      assert(empties.exists(_.endsWith("/parent")))
    }
  }

  test("7.toHadoopPath") {
    val p1 = Url.parse("abc/efg")
    val p2 = p1 / "hij" / "kml"
    assert(toHadoopPath(p2).toString == "abc/efg/hij/kml")
    val p3 = Url.parse("./abc/efg")
    val p4 = p3 / "hij" / "" / "kml"
    assert(toHadoopPath(p4).toString == "abc/efg/hij/kml")

    val p5 = Url.parse("s3://bucket/key")
    val p6 = p5 / "abc/efg"
    assert(toHadoopPath(p6).toString == "s3a://bucket/key/abc/efg")

    val p7 = Url.parse("s3a://bucket/key/")
    val p8 = p7 / 1 / 2 / 3
    assert(toHadoopPath(p8).toString == "s3a://bucket/key/1/2/3")

    val p9 = Url.parse("abc/efg/")
    assert(toHadoopPath(p9).toString == "abc/efg")
  }

  test("8.extract date") {
    val date = LocalDate.of(2025, 8, 10)
    val p1 = path / ymd(date)
    val p2 = path / ymd(date) / "abc" / "xyz.dat"
    assert(extractDate(p1).get == date)
    assert(extractDate(p2).get == date)
  }

  test("9.extract date - should be a valid date") {
    val p1 = path / "Year=2025" / "Month=01" / "Day=35"
    assert(extractDate(p1).isEmpty)
  }

  test("10.extract date - Month should be two digital") {
    val p1 = path / "Year=2025" / "Month=1" / "Day=30"
    assert(extractDate(p1).isEmpty)
  }

  test("11.extract date - year month day should be consecutive") {
    val p1 = path / "Year=2025" / "ooo" / "Month=01" / "Day=30"
    assert(extractDate(p1).isEmpty)
  }

  test("12.retention status") {
    import io.circe.syntax.EncoderOps
    import cats.syntax.show.given
    val frs = FolderRetentionResult(path, Retained)
    println(frs.asJson)
    println(frs.status.show)
  }

  test("13.date folder retention removes stale partitions") {
    val retentionRoot = path / "retention" / "isolated"

    val keep = retentionRoot / ymd(LocalDate.of(2025, 8, 10))
    val stale = retentionRoot / ymd(LocalDate.of(2024, 8, 10))

    for {
      _ <- hdp.delete(retentionRoot)
      _ <- IO {
        File(keep.toString()).createDirectories()
        File(stale.toString()).createDirectories()
        File((keep / "data.txt").toString()).createFileIfNotExists(createParents = true)
        File((stale / "old.txt").toString()).createFileIfNotExists(createParents = true)
      }
      result <- hdp.dateFolderRetention(retentionRoot, List(LocalDate.of(2025, 8, 10)))
      keepExists <- hdp.exists(keep)
      staleExists <- hdp.exists(stale)
    } yield {
      assert(result.exists(_.status == Retained))
      assert(result.exists(_.status == Removed))
      assert(keepExists)
      assert(!staleExists)
    }
  }

  test("14.missing paths are handled as empty") {
    val missingRoot = path / "retention" / "missing"

    for {
      exists <- hdp.exists(missingRoot)
      files <- hdp.filesIn(missingRoot)
      dataFolders <- hdp.dataFolders(missingRoot)
      emptyFolders <- hdp.emptyFolders(missingRoot)
      retention <- hdp.dateFolderRetention(missingRoot, List(LocalDate.of(2025, 8, 10)))
    } yield {
      assert(!exists)
      assert(files.isEmpty)
      assert(dataFolders.isEmpty)
      assert(emptyFolders.isEmpty)
      assert(retention.isEmpty)
    }
  }

  test("15.copy keeps source and overwrites target") {
    val root = path / "copy-move" / "copy-case"
    val source = root / "source.txt"
    val target = root / "target.txt"

    for {
      _ <- hdp.delete(root)
      _ <- IO {
        File(source.toString()).createFileIfNotExists(createParents = true).overwrite("copied-content")
        File(target.toString()).createFileIfNotExists(createParents = true).overwrite("old-content")
      }
      copied <- hdp.copy(source, target)
      sourceExists <- hdp.exists(source)
      targetExists <- hdp.exists(target)
    } yield {
      assert(copied)
      assert(sourceExists)
      assert(targetExists)
      assert(File(target.toString()).contentAsString == "copied-content")
    }
  }

  test("16.move deletes source and keeps target") {
    val root = path / "copy-move" / "move-case"
    val source = root / "source.txt"
    val target = root / "target.txt"

    for {
      _ <- hdp.delete(root)
      _ <- IO(File(source.toString()).createFileIfNotExists(createParents = true).overwrite("moved-content"))
      moved <- hdp.move(source, target)
      sourceExists <- hdp.exists(source)
      targetExists <- hdp.exists(target)
    } yield {
      assert(moved)
      assert(!sourceExists)
      assert(targetExists)
      assert(File(target.toString()).contentAsString == "moved-content")
    }
  }

  test("17.FileSource.bytes rejects sub-byte buffer size") {
    intercept[IllegalArgumentException] {
      hdp.source(path / "any.txt").bytes(squants.information.Bytes(0))
    }
  }

  test("18.extractDate - Year as first path segment") {
    val p1 = Url.parse("Year=2025") / "Month=03" / "Day=15"
    assert(extractDate(p1).get == LocalDate.of(2025, 3, 15))
  }

  test("19.rotateSink rejects size=0") {
    intercept[IllegalArgumentException] {
      hdp.rotateSink(java.time.ZoneId.systemDefault(), 0L)(_ => path)
    }
  }

  test("20.rotateSink rejects negative size") {
    intercept[IllegalArgumentException] {
      hdp.rotateSink(java.time.ZoneId.systemDefault(), -1L)(_ => path)
    }
  }
}
