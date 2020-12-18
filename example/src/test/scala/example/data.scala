package example

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.spark.kafka.{NJProducerRecord, OptionalKV}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

object data {
  private val topicName: String   = topic.topicName.value
  private val timestampRoot: Long = NJTimestamp.now().milliseconds

  val crs: List[OptionalKV[Int, String]] = List(
    OptionalKV(0, 0, timestampRoot + 1, None, Some("a"), topicName, 0),
    OptionalKV(0, 1, timestampRoot + 2, Some(1), None, topicName, 0),
    OptionalKV(0, 2, timestampRoot + 3, Some(2), Some("b"), topicName, 0),
    OptionalKV(0, 3, timestampRoot + 4, Some(3), Some("c"), topicName, 0),
    OptionalKV(0, 4, timestampRoot + 5, None, None, topicName, 0)
  )

  val crRdd: RDD[OptionalKV[Int, String]]    = sparkSession.sparkContext.parallelize(crs)
  val crDS: Dataset[OptionalKV[Int, String]] = ate.normalize(crRdd).dataset

  val prs: List[NJProducerRecord[Int, String]]  = crs.map(_.toNJProducerRecord)
  val prRdd: RDD[NJProducerRecord[Int, String]] = sparkSession.sparkContext.parallelize(prs)

}
