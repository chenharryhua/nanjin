package com.github.chenharryhua.nanjin.terminals

import cats.effect.kernel.Sync
import io.circe.{Decoder, Encoder}
import kantan.csv.{CsvConfiguration, HeaderDecoder}
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.*
import org.apache.parquet.hadoop.util.HiddenFileFilter

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

  // sources and sinks
  def bytes: NJBytes[F]                     = NJBytes[F](config)
  def avro(schema: Schema): NJAvro[F]       = NJAvro[F](config, schema)
  def jackson(schema: Schema): NJJackson[F] = NJJackson[F](config, schema)
  def binAvro(schema: Schema): NJBinAvro[F] = NJBinAvro[F](config, schema)
  def parquet(schema: Schema): NJParquet[F] = NJParquet[F](config, schema)
  def kantan[A: NJHeaderEncoder: HeaderDecoder](csvConfig: CsvConfiguration): NJKantan[F, A] =
    NJKantan[F, A](config, csvConfig)
  def circe[A: Encoder: Decoder](isKeepNull: Boolean): NJCirce[F, A] =
    NJCirce[F, A](config, isKeepNull)
}
