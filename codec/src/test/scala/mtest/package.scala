import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig

package object mtest {

  val sr: Map[String, String] =
    Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://localhost:8081")
}
