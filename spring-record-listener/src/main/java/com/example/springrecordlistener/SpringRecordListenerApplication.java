package com.example.springrecordlistener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

@Slf4j
@SpringBootApplication
public class SpringRecordListenerApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringRecordListenerApplication.class, args);
	}

	/**
	 * 가장 기본적인 리스너 선언
	 * 파라미터로 컨슈머 레코드를 받기 때문에 키, 메시지 값에 대한 처리 가능
	 * @param record
	 */
	@KafkaListener(topics = "test", groupId = "test-group-00")
	public void recordListener(ConsumerRecord<String, String> record){
		log.info("record.toString() : {}",  record.toString());
	}

	/**
	 * 파라미터로 메시지 값을 받는다.
	 * @param messageValue
	 */
	@KafkaListener(topics = "test", groupId = "test-group-01")
	public void singleTopicListener(String messageValue){
		log.info("messageValue : {}", messageValue);
	}

	/**
	 * 개별 리스너에서 옵션값을 부여하고 싶으면 properties를 활용하면 된다.
	 * max.poll.interval.ms : poll() 최대 지연 시 (기본 30000)
	 * auto.offset.reset : 초기 오프셋이나 현재 오프셋이 존재하지 않을경우, earliest로 설
	 * @param messageValue
	 */
	@KafkaListener(topics = "test",
			groupId = "test-group-02" ,
			properties = {
			"max.poll.interval.ms:60000",
			"auto.offset.reset:earliest"})
	public void singleTopicWithPropertiesListener(String messageValue){
		log.info("messageValue : {}", messageValue);
	}


	/**
	 * concurrency 옵션값에 해당하는 만큼 컨슈머 스레드를 만들어서 병렬처리한다.
	 * 예를들어, 파티션이 10개인 토픽을 구독할 경우 가장 좋은 효율을 내기 위해 concurrency를 10으로 설정
	 * 10개의 파티션에 10개의 컨슈머 스레드가 각각 할당돼서 병렬처
	 * @param messageValue
	 */
	@KafkaListener(topics = "test",
	groupId = "test-group-03",
	concurrency = "3")
	public void concurrentTopicListener(String messageValue){
		log.info("messageValue : {}", messageValue);
	}

	/** 특정 토픽의 특정 파티션만 구독할 경우 사용
	 * 추가적으로, PartitionOffset 어노테이션을 사용하면 특정 파티션의 특정 오프셋까지 지정 가능
	 * @param record
	 */
	@KafkaListener(topicPartitions = {
			@TopicPartition(topic = "test01", partitions = {"0", "1"}),
			@TopicPartition(topic = "test02", partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "3"))
			},
			groupId = "test-group-04")
	public void listenSpecificPartition(ConsumerRecord<String,String> record){
		log.info("record : {}", record.toString());
	}
}
