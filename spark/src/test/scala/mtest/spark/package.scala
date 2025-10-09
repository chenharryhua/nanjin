package mtest

import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.spark.SparkSettings
import org.apache.spark.sql.SparkSession

package object spark {

  val sparkSession: SparkSession =
    SparkSettings(sydneyTime)
      .withAppName("nj.spark.test")
      .withMaster("local[*]")
      .withAwsKms("kms")
      .withAwsS3
      .updateConfig(
        _.config("spark.hadoop.fs.ftp.host", "localhost")
          .config("spark.hadoop.fs.ftp.user.localhost", "chenh")
          .config("spark.hadoop.fs.ftp.password.localhost", "test")
          .config("spark.hadoop.fs.ftp.data.connection.mode", "PASSIVE_LOCAL_DATA_CONNECTION_MODE")
          .config("spark.hadoop.fs.ftp.impl", "org.apache.hadoop.fs.ftp.FTPFileSystem"))
      .sparkSession
}
