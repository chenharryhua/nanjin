package com.github.chenharryhua.nanjin.terminals

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Sync}
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.traverse.given
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy}
import fs2.Stream
import io.lemonlabs.uri.{Uri, Url}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.*
import org.apache.parquet.hadoop.util.HiddenFileFilter

import java.time.{LocalDate, ZoneId}
import scala.annotation.tailrec
import scala.collection.mutable

/** Filesystem operations over any Hadoop-compatible store (HDFS, local, S3A, etc.), derived from a Hadoop
  * Configuration.
  *
  * Provides path administration (delete/exists/listing/retention), partition selection helpers
  * (latest/earliest year-month-day[-hour]), file copy/move, and factories for typed file sources, sinks, and
  * rotating sinks. All operations are effectful and run when the returned value is executed.
  *
  * Obtain one via Hadoop[F](configuration).
  *
  * @tparam F
  *   effect type
  */
sealed trait Hadoop[F[_]] {

  /** Delete a path recursively. */
  def delete(path: Url)(using F: Sync[F]): F[Boolean]

  /** Check whether a path exists. */
  def exists(path: Url)(using F: Sync[F]): F[Boolean]

  /** Recursively list all files under the given path. */
  def locatedFileStatus(path: Url)(using F: Sync[F]): F[List[LocatedFileStatus]]

  /** Retrieve all folders that contain at least one file under the given path. Non-leaf directories that
    * contain no files are excluded.
    */
  def dataFolders(path: Url)(using F: Sync[F]): F[List[Url]]

  /** List folders that contain no entries under the given path. */
  def emptyFolders(path: Url)(using F: Sync[F]): F[List[Url]]

  /** List files directly under a path, sorted by modification time, filtered by the given path filter. If the
    * path is a file, it is returned as a single-element list.
    */
  def filesIn(path: Url, filter: PathFilter)(using F: Sync[F]): F[List[Url]]

  /** List files directly under a path (hidden files excluded), sorted by modification time. */
  def filesIn(path: Url)(using F: Sync[F]): F[List[Url]]

  /** Select the best matching sub-path by traversing directories level by level, choosing the max extracted
    * value at each step (per the given ordering). Useful for selecting latest/earliest partitions.
    *
    * @param path
    *   root path
    * @param rules
    *   non-empty list of directory-name parsers, one per level
    * @param Ord
    *   ordering for extracted values
    * @return
    *   best matching path, if any
    */
  def best[T](path: Url, rules: NonEmptyList[String => Option[T]])(using
    F: Sync[F],
    Ord: Ordering[T]): F[Option[Url]]

  /** The deepest path selecting the latest year/month/day partitions, if any. */
  def latestYmd(path: Url)(using F: Sync[F]): F[Option[Url]]

  /** The deepest path selecting the latest year/month/day/hour partitions, if any. */
  def latestYmdh(path: Url)(using F: Sync[F]): F[Option[Url]]

  /** The deepest path selecting the earliest year/month/day partitions, if any. */
  def earliestYmd(path: Url)(using F: Sync[F]): F[Option[Url]]

  /** The deepest path selecting the earliest year/month/day/hour partitions, if any. */
  def earliestYmdh(path: Url)(using F: Sync[F]): F[Option[Url]]

  /** Apply date-based retention on folders. Folders whose extracted date is not in keeps are deleted; folders
    * that do not represent a date are retained.
    *
    * @param path
    *   root path
    * @param keeps
    *   dates to retain
    */
  def dateFolderRetention(path: Url, keeps: List[LocalDate])(using F: Sync[F]): F[List[FolderRetentionResult]]

  /** Retain folders from startFrom going back backwardDays; delete the rest.
    *
    * @param startFrom
    *   start date
    * @param backwardDays
    *   number of days to keep, counting back from startFrom
    */
  def dateFolderRetention(path: Url, startFrom: LocalDate, backwardDays: Long)(using
    F: Sync[F]): F[List[FolderRetentionResult]]

  /** Copy a file or directory to another Hadoop-compatible path, preserving the source.
    *
    * A thin wrapper over Hadoop FileUtil.copy. On object stores such as S3 the operation is not atomic and a
    * partial failure may leave incomplete objects at the target; use a storage-specific SDK if you need
    * stronger guarantees.
    */
  def copy(source: Url, target: Url)(using F: Sync[F]): F[Boolean]

  /** Move a file or directory to another Hadoop-compatible path, removing the source after a successful
    * transfer.
    *
    * A thin wrapper over Hadoop FileUtil.copy with source deletion. On object stores such as S3 move is a
    * copy followed by delete and is not atomic; a partial failure may leave orphaned objects at either
    * location.
    */
  def move(source: Url, target: Url)(using F: Sync[F]): F[Boolean]

  /** A typed file source rooted at url, exposing the format-specific readers. */
  def source(url: Url)(using F: Sync[F]): FileSource[F]

  /** A typed file sink rooted at url, exposing the format-specific writers. */
  def sink(url: Url)(using F: Sync[F]): FileSink[F]

  /** A rotating sink that starts a new file on each tick of the time policy.
    *
    * @param zoneId
    *   time zone driving the policy ticks
    * @param f
    *   rotation policy
    * @param pathBuilder
    *   builds the output path for each rotation
    */
  def rotateSink(zoneId: ZoneId, f: Policy.type => Policy)(pathBuilder: CreateRotateFile => Url)(using
    F: Async[F]): RotateByPolicy[F]

  /** A rotating sink that starts a new file every size elements.
    *
    * @param zoneId
    *   time zone for naming/timestamps
    * @param size
    *   maximum elements per file; must be positive
    * @param pathBuilder
    *   builds the output path for each rotation
    */
  def rotateSink(zoneId: ZoneId, size: Long)(pathBuilder: CreateRotateFile => Url)(using
    F: Async[F]): RotateBySize[F]
}

object Hadoop {
  def apply[F[_]](config: Configuration): Hadoop[F] =
    new HadoopImpl[F](config)
}

final private class HadoopImpl[F[_]](config: Configuration) extends Hadoop[F] {

  override def delete(path: Url)(using F: Sync[F]): F[Boolean] =
    F.blocking {
      val hp: Path = toHadoopPath(path)
      val fs: FileSystem = hp.getFileSystem(config)
      fs.delete(hp, true)
    }

  override def exists(path: Url)(using F: Sync[F]): F[Boolean] =
    F.blocking {
      val hp: Path = toHadoopPath(path)
      val fs: FileSystem = hp.getFileSystem(config)
      fs.exists(hp)
    }

  override def locatedFileStatus(path: Url)(using F: Sync[F]): F[List[LocatedFileStatus]] =
    F.blocking {
      val hp: Path = toHadoopPath(path)
      val fs: FileSystem = hp.getFileSystem(config)
      if (!fs.exists(hp)) Nil
      else {
        val ri: RemoteIterator[LocatedFileStatus] = fs.listFiles(hp, true)
        val lb = mutable.ListBuffer.empty[LocatedFileStatus]
        while (ri.hasNext) lb.addOne(ri.next()) // scalafix:ok
        lb.toList
      }
    }

  override def dataFolders(path: Url)(using F: Sync[F]): F[List[Url]] =
    F.blocking {
      val hp: Path = toHadoopPath(path)
      val fs: FileSystem = hp.getFileSystem(config)
      if (!fs.exists(hp)) Nil
      else {
        val ri: RemoteIterator[LocatedFileStatus] = fs.listFiles(hp, true)
        val lb: mutable.Set[Path] = collection.mutable.Set.empty

        while (ri.hasNext) lb.addOne(ri.next().getPath.getParent) // scalafix:ok

        lb.toList.map(p => Uri(p.toUri).toUrl)
      }
    }

  override def emptyFolders(path: Url)(using F: Sync[F]): F[List[Url]] =
    F.blocking {
      val hp: Path = toHadoopPath(path)
      val fs: FileSystem = hp.getFileSystem(config)

      if (!fs.exists(hp)) Nil
      else {
        val result = mutable.ListBuffer.empty[Path]
        val stack = mutable.Stack(hp)

        while (stack.nonEmpty) { // scalafix:ok
          val current = stack.pop()
          val status = fs.getFileStatus(current)

          if (status.isDirectory) {
            val children = fs.listStatus(current)
            if (children.isEmpty) {
              result += current
            } else {
              children.foreach { child =>
                if (child.isDirectory)
                  stack.push(child.getPath)
              }
            }
          }
        }
        result.toList.map(p => Uri(p.toUri).toUrl)
      }
    }

  override def filesIn(path: Url, filter: PathFilter)(using F: Sync[F]): F[List[Url]] =
    F.blocking {
      val hp: Path = toHadoopPath(path)
      val fs: FileSystem = hp.getFileSystem(config)
      if (!fs.exists(hp)) Nil
      else {
        val stat: FileStatus = fs.getFileStatus(hp)
        if (stat.isFile)
          List(Uri(stat.getPath.toUri).toUrl)
        else
          fs.listStatus(hp, filter)
            .filter(_.isFile)
            .sortBy(_.getModificationTime)
            .map(s => Uri(s.getPath.toUri).toUrl)
            .toList
      }
    }

  override def filesIn(path: Url)(using F: Sync[F]): F[List[Url]] =
    filesIn(path, HiddenFileFilter.INSTANCE)

  override def best[T](path: Url, rules: NonEmptyList[String => Option[T]])(using
    F: Sync[F],
    Ord: Ordering[T]): F[Option[Url]] =
    F.blocking {
      val hp: Path = toHadoopPath(path)
      val fs: FileSystem = hp.getFileSystem(config)
      if (!fs.exists(hp)) None
      else {
        @tailrec
        def go(hp: Path, js: List[String => Option[T]]): Option[Path] =
          js match {
            case f :: tail =>
              fs.listStatus(hp)
                .filter(_.isDirectory)
                .flatMap(s => f(s.getPath.getName).map((_, s)))
                .maxByOption(_._1)
                .map(_._2) match {
                case Some(status) => go(status.getPath, tail)
                case None         => None
              }
            case Nil => Some(hp)
          }
        go(hp, rules.toList).map(p => Uri(p.toUri).toUrl)
      }
    }

  override def latestYmd(path: Url)(using F: Sync[F]): F[Option[Url]] = {
    import partitionPath.{day, month, year}
    best[Int](path, NonEmptyList.of(year, month, day))
  }

  override def latestYmdh(path: Url)(using F: Sync[F]): F[Option[Url]] = {
    import partitionPath.{day, hour, month, year}
    best[Int](path, NonEmptyList.of(year, month, day, hour))
  }

  override def earliestYmd(path: Url)(using F: Sync[F]): F[Option[Url]] = {
    import partitionPath.{day, month, year}
    best(path, NonEmptyList.of[String => Option[Int]](year, month, day))(using F, Ordering[Int].reverse)
  }

  override def earliestYmdh(path: Url)(using F: Sync[F]): F[Option[Url]] = {
    import partitionPath.{day, hour, month, year}
    best(path, NonEmptyList.of[String => Option[Int]](year, month, day, hour))(using F, Ordering[Int].reverse)
  }

  override def dateFolderRetention(path: Url, keeps: List[LocalDate])(using
    F: Sync[F]): F[List[FolderRetentionResult]] =
    dataFolders(path).flatMap(_.traverse { url =>
      extractDate(url) match {
        case Some(date) =>
          if (keeps.contains(date))
            F.pure(FolderRetentionResult(url, RetentionStatus.Retained))
          else {
            delete(url).map {
              case true  => FolderRetentionResult(url, RetentionStatus.Removed)
              case false => FolderRetentionResult(url, RetentionStatus.RemoveFailed)
            }
          }
        case None => F.pure(FolderRetentionResult(url, RetentionStatus.Retained))
      }
    })

  override def dateFolderRetention(path: Url, startFrom: LocalDate, backwardDays: Long)(using
    F: Sync[F]): F[List[FolderRetentionResult]] = {
    val keeps = (0L until backwardDays).map(startFrom.minusDays).toList
    dateFolderRetention(path, keeps)
  }

  private def copy_file(source: Url, target: Url, delete_source: Boolean)(using F: Sync[F]): F[Boolean] =
    F.blocking {
      val src = toHadoopPath(source)
      val tgt = toHadoopPath(target)

      val srcFs = src.getFileSystem(config)
      val tgtFs = tgt.getFileSystem(config)

      FileUtil.copy(srcFs, src, tgtFs, tgt, delete_source, true, config)
    }

  override def copy(source: Url, target: Url)(using F: Sync[F]): F[Boolean] =
    copy_file(source, target, false)

  override def move(source: Url, target: Url)(using F: Sync[F]): F[Boolean] =
    copy_file(source, target, true)

  override def source(url: Url)(using F: Sync[F]): FileSource[F] = new FileSourceImpl[F](config, url)

  override def sink(url: Url)(using F: Sync[F]): FileSink[F] = new FileSinkImpl[F](config, url)

  override def rotateSink(zoneId: ZoneId, f: Policy.type => Policy)(pathBuilder: CreateRotateFile => Url)(
    using F: Async[F]): RotateByPolicy[F] = {
    val crfs: Stream[F, CreateRotateFile] =
      tickStream.tickFuture[F](zoneId, f).map { tick =>
        CreateRotateFile(tick.sequenceId, tick.index, tick.zoned(_.acquires))
      }
    new RotateByPolicyImpl[F](config, pathBuilder, crfs)
  }

  override def rotateSink(zoneId: ZoneId, size: Long)(pathBuilder: CreateRotateFile => Url)(using
    F: Async[F]): RotateBySize[F] = {
    require(size > 0L, "size must be positive")
    new RotateBySizeImpl[F](config, zoneId, pathBuilder, size)
  }
}
