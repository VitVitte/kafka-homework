import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.LocalDateTime


fun main() {
    val logs = LoggerFactory.getLogger("appl")


    val producerConfig1: Map<String, Any> = ProducerTransactionUtils.getProducerConfig(
        mapOf(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "my_transaction_id1",
        )
    )

    val producerConfig2: Map<String, Any> = ProducerTransactionUtils.getProducerConfig(
        mapOf(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG to "my_transaction_id2",
        )
    )

    val producer1 = KafkaProducer<String, String>(producerConfig1)
    val producer2 = KafkaProducer<String, String>(producerConfig2)

    producer1.initTransactions()
    producer2.initTransactions()


    for (i in 1..5) {
        producer1.beginTransaction()
        producer2.beginTransaction()

        val record1 = ProducerRecord("mytopic1", "$i", "value " + LocalDateTime.now())
        producer1.send(record1) { recordMetadata: RecordMetadata?, exception: Exception? ->
            logs.info("[1_1] Sent $i - ${Instant.now()}; Message timestamp - [${Instant.ofEpochMilli(recordMetadata?.timestamp() ?: 0L)}]")
        }

        val record2 = ProducerRecord("mytopic2", "$i", "value " + LocalDateTime.now())
        producer2.send(record2) { recordMetadata: RecordMetadata?, exception: Exception? ->
            logs.info("[2_1] Sent $i - ${Instant.now()}; Message timestamp - [${Instant.ofEpochMilli(recordMetadata?.timestamp() ?: 0L)}]")
        }

        producer1.commitTransaction()
        producer2.commitTransaction()
    }

    for (i in 1..2) {
        producer1.beginTransaction()
        producer2.beginTransaction()

        val record1 = ProducerRecord("mytopic1", "$i", "value " + LocalDateTime.now())
        producer1.send(record1) { recordMetadata: RecordMetadata?, exception: Exception? ->
            logs.info("[1_2] Sent $i - ${Instant.now()}; Message timestamp - [${Instant.ofEpochMilli(recordMetadata?.timestamp() ?: 0L)}]")
        }

        val record2 = ProducerRecord("mytopic2", "$i", "value " + LocalDateTime.now())
        producer2.send(record2) { recordMetadata: RecordMetadata?, exception: Exception? ->
            logs.info("[2_2] Sent $i - ${Instant.now()}; Message timestamp - [${Instant.ofEpochMilli(recordMetadata?.timestamp() ?: 0L)}]")
        }

        producer1.abortTransaction()
        producer2.abortTransaction()
    }

    producer1.flush()
    producer2.flush()

    producer1.close()
    producer2.close()

    System.out.printf("Message send")
}


object ProducerTransactionUtils {
    const val HOST: String = "localhost:9092"
    private val producerConfig = mapOf<String, Any>(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to HOST,
        ProducerConfig.TRANSACTIONAL_ID_CONFIG to "my_transaction_id",
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to "true",
        ProducerConfig.ACKS_CONFIG to "all",
        ProducerConfig.RETRIES_CONFIG to 2147483647,
        ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION to 5,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
    )

    fun getProducerConfig(addConfigs: Map<String, Any>? = null): Map<String, Any> {
        val resultConfig = HashMap<String, Any>(producerConfig)

        addConfigs?.let {
            resultConfig += addConfigs
        }
        return resultConfig
    }
}