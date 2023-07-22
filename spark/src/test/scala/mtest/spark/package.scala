package mtest

import com.github.chenharryhua.nanjin.common.time.zones.sydneyTime
import com.github.chenharryhua.nanjin.spark.SparkSettings
import org.apache.spark.sql.SparkSession

package object spark {

  val sparkSession: SparkSession =
    SparkSettings(sydneyTime)
      .withAppName("nj.spark.test")
      .withoutUI
      .updateConfig(
        _.set("spark.hadoop.fs.ftp.host", "localhost")
          .set("spark.hadoop.fs.ftp.user.localhost", "chenh")
          .set("spark.hadoop.fs.ftp.password.localhost", "test")
          .set("spark.hadoop.fs.ftp.data.connection.mode", "PASSIVE_LOCAL_DATA_CONNECTION_MODE")
          .set("spark.hadoop.fs.ftp.impl", "org.apache.hadoop.fs.ftp.FTPFileSystem"))
      .sparkSession
}
