package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.github.chenharryhua.nanjin.kafka.codec.{KJson, ManualAvroSchema}
import com.landoop.telecom.telecomitalia.telecommunications.{smsCallInternet, Key}
import io.circe.generic.auto._

class TopicNameSyntaxTest {
  val topic1 = ctx.topic[KJson[PKey], Payment](TopicName("topic1"))
  val topic2 = ctx.topic[PKey, KJson[Payment]](TopicName("topic2"))
  val topic3 = ctx.topic[Int, Int](TopicName("topic3"))
  val tooic4 = ctx.topic(TopicDef[Int, KJson[Payment]](TopicName("topic4")))
  val topic5 = ctx.topic(TopicDef[Int, Payment](TopicName("topic5")))
  val topic6 = TopicDef[Int, Int](TopicName("topic6")).in(ctx)

  val topic7 = TopicDef(
    TopicName("telecom_italia_data"),
    ManualAvroSchema[Key](Key.schema),
    ManualAvroSchema[smsCallInternet](smsCallInternet.schema))

//  val topic8 =
//    TopicDef[Key, smsCallInternet]("telecom_italia_data", ManualAvroSchema[Key](Key.schema)).in(ctx)

  val topic9 = TopicDef[Key, smsCallInternet](
    TopicName("telecom_italia_data"),
    ManualAvroSchema[smsCallInternet](smsCallInternet.schema)).in(ctx)

}
