import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

@Slf4j
public class SyncCallbackProducer {
    private final static String TOPIC_NAME = "test";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(PropertiesFactory.getProperties());

        //토픽명, 메세지, 키 지정, 파티션 번호
        int partitionNo = 0;
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partitionNo, "Pangyo",  "23");

        RecordMetadata metadata = producer.send(record).get();
        log.info("metadata = "+ metadata.toString());
        log.info("{}", record);
        producer.flush();
        producer.close();
    }
}
