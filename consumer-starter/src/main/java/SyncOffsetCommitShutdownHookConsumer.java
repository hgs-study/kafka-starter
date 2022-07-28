import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Properties;

/**
 * 컨슈머 안전한 종료
 */
@Slf4j
public class SyncOffsetCommitShutdownHookConsumer {
    public static void main(String[] args) {
        Properties properties = PropertiesFactory.getProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        Runtime.getRuntime().addShutdownHook(new ShutdownThread(consumer));
        try {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("record : {}", record);
                }
            }
        }catch (WakeupException e){
            log.warn("Wakeup consumer");
        }finally {
            // 리소스 종료
            consumer.close();
        }
    }

    /**
     * 사용자가 kill-TERM {프로세스 번호} 를 호출해서 셧다운 발생시킬 수 있다.
     * 아래 정의한 스레드가 실행하면서 wakeup() 메서드가 호출되어 컨슈머가 안전하게 종료
     */
    static class ShutdownThread extends Thread{

        private final KafkaConsumer<String,String> consumer;

        public ShutdownThread(KafkaConsumer<String,String> consumer){
            super();
            this.consumer = consumer;
        }

        @Override
        public void run() {
            log.info("Shutdwon hook");
            consumer.wakeup();
        }
    }
}
