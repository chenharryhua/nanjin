package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.Sync
import kantan.csv.CsvConfiguration
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.*
import org.apache.parquet.hadoop.util.HiddenFileFilter

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object NJHadoop {

  def apply[F[_]](config: Configuration): NJHadoop[F] = new NJHadoop[F](config)
}

final class NJHadoop[F[_]] private (config: Configuration) {

  // disk operations

  def delete(path: NJPath)(implicit F: Sync[F]): F[Boolean] = F.blocking {
    val fs: FileSystem = path.hadoopPath.getFileSystem(config)
    fs.delete(path.hadoopPath, true)
  }

  def isExist(path: NJPath)(implicit F: Sync[F]): F[Boolean] = F.blocking {
    val fs: FileSystem = path.hadoopPath.getFileSystem(config)
    fs.exists(path.hadoopPath)
  }

  def locatedFileStatus(path: NJPath)(implicit F: Sync[F]): F[List[LocatedFileStatus]] = F.blocking {
    val fs: FileSystem                        = path.hadoopPath.getFileSystem(config)
    val ri: RemoteIterator[LocatedFileStatus] = fs.listFiles(path.hadoopPath, true)
    val lb: ListBuffer[LocatedFileStatus]     = ListBuffer.empty[LocatedFileStatus]
    while (ri.hasNext) lb.addOne(ri.next())
    lb.toList
  }

  /** retrieve all folder names which contain files under path folder
    * @param path
    * @return
    */
  def dataFolders(path: NJPath)(implicit F: Sync[F]): F[List[NJPath]] = F.blocking {
    val fs: FileSystem                        = path.hadoopPath.getFileSystem(config)
    val ri: RemoteIterator[LocatedFileStatus] = fs.listFiles(path.hadoopPath, true)
    val lb: mutable.Set[Path]                 = collection.mutable.Set.empty
    while (ri.hasNext) lb.addOne(ri.next().getPath.getParent)
    lb.toList.map(NJPath(_))
  }

  /** retrieve file name under path folder, sorted by modification time
    * @param path
    * @return
    */
  def filesIn(path: NJPath)(implicit F: Sync[F]): F[List[NJPath]] = F.blocking {
    val fs: FileSystem   = path.hadoopPath.getFileSystem(config)
    val stat: FileStatus = fs.getFileStatus(path.hadoopPath)
    if (stat.isFile)
      List(NJPath(stat.getPath))
    else
      fs.listStatus(path.hadoopPath, HiddenFileFilter.INSTANCE)
        .filter(_.isFile)
        .sortBy(_.getModificationTime)
        .map(s => NJPath(s.getPath))
        .toList
  }

  /** @param path
    *   the root path where search starts
    * @param rules
    *   list of rules
    * @return
    *   the best path according to the rules
    */
  def latest[T](path: NJPath, rules: List[String => Option[T]])(implicit
    F: Sync[F],
    Ord: Ordering[T]): F[Option[NJPath]] =
    F.blocking {
      val fs: FileSystem = path.hadoopPath.getFileSystem(config)
      @tailrec
      def go(hp: Path, js: List[String => Option[T]]): Option[Path] =
        js match {
          case f :: tail =>
            fs.listStatus(hp)
              .flatMap(s => f(s.getPath.getName).map((_, s)))
              .maxByOption(_._1)
              .map(_._2) match {
              case Some(status) => go(status.getPath, tail)
              case None         => None
            }
          case Nil => Some(hp)
        }
      go(path.hadoopPath, rules).map(NJPath(_))
    }

  // sources and sinks
  def bytes: HadoopBytes[F]                              = HadoopBytes[F](config)
  def avro(schema: Schema): HadoopAvro[F]                = HadoopAvro[F](config, schema)
  def jackson(schema: Schema): HadoopJackson[F]          = HadoopJackson[F](config, schema)
  def binAvro(schema: Schema): HadoopBinAvro[F]          = HadoopBinAvro[F](config, schema)
  def parquet(schema: Schema): HadoopParquet[F]          = HadoopParquet[F](config, schema)
  def kantan(csvConf: CsvConfiguration): HadoopKantan[F] = HadoopKantan[F](config, csvConf)
  def circe: HadoopCirce[F]                              = HadoopCirce[F](config)
}
