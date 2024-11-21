import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.IsolationLevel
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration

fun main() {
    val groupId = "app-group"
    val topics = listOf<String>("mytopic1", "mytopic2")

    val consumerConfig: Map<String, Any> = ConsumerTransactionUtils.getConsumerConfig(
        mapOf(
            ConsumerConfig.GROUP_ID_CONFIG to groupId,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        )
    )

    val consumer: KafkaConsumer<String, String> = KafkaConsumer<String, String>(consumerConfig)

    ConsumerTransactionUtils.log.warn("Subscribe to ${topics} with ${groupId}")

    consumer.subscribe(topics)

    try {
        while (true) {

            val result: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(10))

            ConsumerTransactionUtils.log.info("Read ${result.count()}")

            result.forEach {
                with(it) {
                    ConsumerTransactionUtils.log.warn("offset=${offset()}, key=${key()}, value=${value()}, headers=${headers()}")
                }
            }
        }
    } finally {
        consumer.close()
    }
}

object ConsumerTransactionUtils {
    val log: Logger = LoggerFactory.getLogger("appl")

    const val HOST: String = "localhost:9092"

    private val consumerConfig = mutableMapOf<String, Any>(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to HOST,
        ConsumerConfig.GROUP_ID_CONFIG to "some-default-consumer-group",
        ConsumerConfig.ISOLATION_LEVEL_CONFIG to IsolationLevel.READ_COMMITTED.toString().lowercase(),
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java
    )

    fun getConsumerConfig(addConfigs: Map<String, Any>? = null): Map<String, Any> {
        val resultConfig = HashMap<String, Any>(consumerConfig)

        addConfigs?.let {
            resultConfig += addConfigs
        }
        return resultConfig
    }
}