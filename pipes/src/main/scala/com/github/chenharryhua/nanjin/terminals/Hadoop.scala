package com.github.chenharryhua.nanjin.terminals

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Sync}
import cats.implicits.{toFlatMapOps, toFunctorOps, toTraverseOps}
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, TickedValue}
import com.github.chenharryhua.nanjin.datetime.codec
import fs2.Stream
import io.lemonlabs.uri.{Uri, Url}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.*
import org.apache.parquet.hadoop.util.HiddenFileFilter

import java.time.{LocalDate, ZoneId}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Hadoop {

  def apply[F[_]](config: Configuration): Hadoop[F] = new Hadoop[F](config)
}

final class Hadoop[F[_]] private (config: Configuration) {

  /** Delete a path recursively.
    *
    * @param path
    *   Path to delete
    * @return
    *   true if deletion succeeded, false otherwise
    */
  def delete(path: Url)(implicit F: Sync[F]): F[Boolean] =
    F.blocking {
      val hp: Path = toHadoopPath(path)
      val fs: FileSystem = hp.getFileSystem(config)
      fs.delete(hp, true)
    }

  /** Check whether a path exists.
    *
    * @param path
    *   Path to check
    */
  def isExist(path: Url)(implicit F: Sync[F]): F[Boolean] =
    F.blocking {
      val hp: Path = toHadoopPath(path)
      val fs: FileSystem = hp.getFileSystem(config)
      fs.exists(hp)
    }

  /** Recursively list all files under the given path.
    *
    * @param path
    *   Root path
    * @return
    *   All located file statuses
    */
  def locatedFileStatus(path: Url)(implicit F: Sync[F]): F[List[LocatedFileStatus]] =
    F.blocking {
      val hp: Path = toHadoopPath(path)
      val fs: FileSystem = hp.getFileSystem(config)
      val ri: RemoteIterator[LocatedFileStatus] = fs.listFiles(hp, true)
      val lb: ListBuffer[LocatedFileStatus] = ListBuffer.empty[LocatedFileStatus]
      while (ri.hasNext) lb.addOne(ri.next()) // scalafix:ok
      lb.toList
    }

  /** Retrieve all folders that contain at least one file under the given path.
    *
    * Non-leaf directories that contain no files are excluded.
    *
    * @param path
    *   Search root
    */
  def dataFolders(path: Url)(implicit F: Sync[F]): F[List[Url]] =
    F.blocking {
      val hp: Path = toHadoopPath(path)
      val fs: FileSystem = hp.getFileSystem(config)
      val ri: RemoteIterator[LocatedFileStatus] = fs.listFiles(hp, true)
      val lb: mutable.Set[Path] = collection.mutable.Set.empty

      while (ri.hasNext) lb.addOne(ri.next().getPath.getParent) // scalafix:ok

      lb.toList.map(p => Uri(p.toUri).toUrl)
    }

  /** List files directly under a path, sorted by modification time.
    *
    * If the path is a file, it is returned as a single-element list.
    *
    * @param path
    *   Root path
    * @param filter
    *   Hadoop path filter
    */
  def filesIn(path: Url, filter: PathFilter)(implicit F: Sync[F]): F[List[Url]] =
    F.blocking {
      val hp: Path = toHadoopPath(path)
      val fs: FileSystem = hp.getFileSystem(config)
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

  def filesIn(path: Url)(implicit F: Sync[F]): F[List[Url]] =
    filesIn(path, HiddenFileFilter.INSTANCE)

  /** Select the best matching sub-path according to a sequence of rules.
    *
    * Each rule extracts an ordering key from a directory name. Directories are traversed level by level,
    * selecting the maximum (or minimum) value at each step.
    *
    * Typical use cases include selecting the latest or earliest year/month/day/hour partition.
    *
    * @param path
    *   Root path
    * @param rules
    *   Non-empty list of directory name parsers
    * @param Ord
    *   Ordering for extracted values
    * @return
    *   Best matching path, if any
    */
  def best[T](path: Url, rules: NonEmptyList[String => Option[T]])(implicit
    F: Sync[F],
    Ord: Ordering[T]): F[Option[Url]] = F.blocking {
    val hp: Path = toHadoopPath(path)
    val fs: FileSystem = hp.getFileSystem(config)
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

  /** @param path
    *   the path where search starts
    * @return
    *   the path which has the latest one or None
    */
  def latestYmd(path: Url)(implicit F: Sync[F]): F[Option[Url]] =
    best[Int](path, NonEmptyList.of(codec.year, codec.month, codec.day))

  def latestYmdh(path: Url)(implicit F: Sync[F]): F[Option[Url]] =
    best[Int](path, NonEmptyList.of(codec.year, codec.month, codec.day, codec.hour))

  def earliestYmd(path: Url)(implicit F: Sync[F]): F[Option[Url]] =
    best(path, NonEmptyList.of[String => Option[Int]](codec.year, codec.month, codec.day))(
      F,
      Ordering[Int].reverse)

  def earliestYmdh(path: Url)(implicit F: Sync[F]): F[Option[Url]] =
    best(path, NonEmptyList.of[String => Option[Int]](codec.year, codec.month, codec.day, codec.hour))(
      F,
      Ordering[Int].reverse)

  /** Apply date-based retention on folders.
    *
    * Folders whose extracted date is not present in `keeps` will be deleted. Folders that do not represent a
    * date are retained.
    *
    * @param path
    *   Root path
    * @param keeps
    *   Dates to retain
    */
  def dateFolderRetention(path: Url, keeps: List[LocalDate])(implicit
    F: Sync[F]): F[List[RetentionFolderStatus]] =
    dataFolders(path).flatMap(_.traverse { url =>
      extractDate(url) match {
        case Some(date) =>
          if (keeps.contains(date))
            F.pure(RetentionFolderStatus(url, RetentionStatus.Retained))
          else {
            delete(url).map {
              case true  => RetentionFolderStatus(url, RetentionStatus.Removed)
              case false => RetentionFolderStatus(url, RetentionStatus.RemovalFailed)
            }
          }
        case None => F.pure(RetentionFolderStatus(url, RetentionStatus.Retained))
      }
    })

  /** Retain folders from ''startFrom'' going back ''days''
    * @param startFrom
    *   start date
    * @param backwardDays
    *   backward days
    * @return
    */
  def dateFolderRetention(path: Url, startFrom: LocalDate, backwardDays: Long)(implicit
    F: Sync[F]): F[List[RetentionFolderStatus]] = {
    val keeps = (0L until backwardDays).map(startFrom.minusDays).toList
    dateFolderRetention(path, keeps)
  }

  /*
   * source and sink
   */

  def source(url: Url)(implicit F: Sync[F]): FileSource[F] = new FileSourceImpl[F](config, url)

  def sink(url: Url)(implicit F: Sync[F]): FileSink[F] = new FileSinkImpl[F](config, url)

  /** Create a policy-based rotating sink.
    *
    * Rotation is driven by an externally provided tick stream.
    */
  def rotateSink(ticks: Stream[F, TickedValue[Url]])(implicit F: Async[F]): RotateByPolicy[F] =
    new RotateByPolicySink[F](config, ticks)

  /** Create a policy-based rotating sink using a time policy.
    *
    * @param zoneId
    *   Time zone
    * @param policy
    *   Rotation policy
    * @param pathBuilder
    *   Builds output paths for each rotation
    */
  def rotateSink(zoneId: ZoneId, policy: Policy)(pathBuilder: CreateRotateFile => Url)(implicit
    F: Async[F]): RotateByPolicy[F] =
    rotateSink(tickStream.tickFuture[F](zoneId, policy).map { tick =>
      val cfe = CreateRotateFile(tick.sequenceId, tick.index, tick.zoned(_.acquires))
      TickedValue(tick, pathBuilder(cfe))
    })

  /** Create a size-based rotating sink.
    *
    * Rotation occurs when the number of elements written reaches `size`.
    *
    * @param zoneId
    *   Time zone
    * @param size
    *   Maximum elements per file (must be > 0)
    */
  def rotateSink(zoneId: ZoneId, size: Long)(pathBuilder: CreateRotateFile => Url)(implicit
    F: Async[F]): RotateBySize[F] = {
    require(size > 0L, "size should be bigger than zero")
    new RotateBySizeSink[F](config, zoneId, pathBuilder, size)
  }
}
