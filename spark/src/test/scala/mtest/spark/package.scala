package mtest

import com.github.chenharryhua.nanjin.datetime.sydneyTime
import com.github.chenharryhua.nanjin.spark.SparkSettings
import org.apache.spark.sql.SparkSession

package object spark {

  val sparkSession: SparkSession =
    SparkSettings(sydneyTime).withAppName("nj.spark.test").withUI.sparkSession
}
