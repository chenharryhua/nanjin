package com.github.chenharryhua.nanjin.terminals

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Sync}
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick, TickedValue}
import com.github.chenharryhua.nanjin.datetime.codec
import fs2.Stream
import io.lemonlabs.uri.{Uri, Url}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.*
import org.apache.parquet.hadoop.util.HiddenFileFilter

import java.time.ZoneId
import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Hadoop {

  def apply[F[_]](config: Configuration): Hadoop[F] = new Hadoop[F](config)
}

final class Hadoop[F[_]] private (config: Configuration) {

  // disk operations

  def delete(path: Url)(implicit F: Sync[F]): F[Boolean] = F.blocking {
    val hp: Path = toHadoopPath(path)
    val fs: FileSystem = hp.getFileSystem(config)
    fs.delete(hp, true)
  }

  def isExist(path: Url)(implicit F: Sync[F]): F[Boolean] = F.blocking {
    val hp: Path = toHadoopPath(path)
    val fs: FileSystem = hp.getFileSystem(config)
    fs.exists(hp)
  }

  /** hadoop listFiles
    * @return
    */
  def locatedFileStatus(path: Url)(implicit F: Sync[F]): F[List[LocatedFileStatus]] = F.blocking {
    val hp: Path = toHadoopPath(path)
    val fs: FileSystem = hp.getFileSystem(config)
    val ri: RemoteIterator[LocatedFileStatus] = fs.listFiles(hp, true)
    val lb: ListBuffer[LocatedFileStatus] = ListBuffer.empty[LocatedFileStatus]
    while (ri.hasNext) lb.addOne(ri.next())
    lb.toList
  }

  /** retrieve all folder names which contain files under path folder
    * @param path
    *   search root
    * @return
    */
  def dataFolders(path: Url)(implicit F: Sync[F]): F[List[Url]] = F.blocking {
    val hp: Path = toHadoopPath(path)
    val fs: FileSystem = hp.getFileSystem(config)
    val ri: RemoteIterator[LocatedFileStatus] = fs.listFiles(hp, true)
    val lb: mutable.Set[Path] = collection.mutable.Set.empty
    while (ri.hasNext) lb.addOne(ri.next().getPath.getParent)
    lb.toList.map(p => Uri(p.toUri).toUrl)
  }

  /** retrieve file name under path folder, sorted by modification time
    * @param path
    *   root
    * @return
    */
  def filesIn(path: Url, filter: PathFilter)(implicit F: Sync[F]): F[List[Url]] = F.blocking {
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

  /** @param path
    *   the root path where search starts
    * @param rules
    *   list of rules. the String looks like Year=2023 or Month=07 or Day=29
    * @return
    *   the best path according to the rules
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
    best(path, NonEmptyList.of(codec.year, codec.month, codec.day))(F, Ordering[Int].reverse)

  def earliestYmdh(path: Url)(implicit F: Sync[F]): F[Option[Url]] =
    best(path, NonEmptyList.of(codec.year, codec.month, codec.day, codec.hour))(F, Ordering[Int].reverse)

  // sources and sinks

  def source(path: Url)(implicit F: Sync[F]): FileSource[F] = FileSource[F](config, path)

  def sink(path: Url)(implicit F: Sync[F]): FileSink[F] = FileSink[F](config, path)

  def rotateSink(ticks: Stream[F, TickedValue[Url]])(implicit F: Async[F]): RotateByPolicy[F] =
    new RotateByPolicySink[F](config, ticks.map(_.map(toHadoopPath)))

  /** Policy based rotation sink
    */
  def rotateSink(policy: Policy, zoneId: ZoneId)(pathBuilder: Tick => Url)(implicit
    F: Async[F]): RotateByPolicy[F] =
    rotateSink(tickStream.fromZero[F](policy, zoneId).map(tick => TickedValue(tick, pathBuilder(tick))))

  /** Size based rotation sink
    */
  def rotateSink(size: Int)(pathBuilder: Tick => Url)(implicit F: Async[F]): RotateBySize[F] = {
    require(size > 0, "size should be bigger than zero")
    new RotateBySizeSink[F](config, pathBuilder.andThen(toHadoopPath), size)
  }
}
