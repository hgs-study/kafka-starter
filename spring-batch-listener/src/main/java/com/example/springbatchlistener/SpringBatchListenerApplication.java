package com.example.springbatchlistener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

@Slf4j
@SpringBootApplication
public class SpringBatchListenerApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchListenerApplication.class, args);
	}

	/**
	 * 카프카 클라이언트로 poll() 메서드로 리턴받아서 사용하는 것과 동일한 형태
	 * 파라미터 : ConsumerRecords
	 * @param records
	 */
	@KafkaListener(topics = "test", groupId = "test-group-01")
	public void batchListener(ConsumerRecords<String, String> records){
		records.forEach(record -> log.info("record : {}", record));
	}

	/**
	 * 카프카 클라이언트로 poll() 메서드로 리턴받아서 사용하는 것과 동일한 형태
	 * 파라미터 : List
	 */
	@KafkaListener(topics = "test", groupId = "test-group-02")
	public void batchListener(List<String> list){
		list.forEach(recordValue -> log.info("recordValue : {}", recordValue));
	}

	/**
	 * 2개 이상의 컨슈머 스레드로 배치 리스너를 운영할 경우
	 * concurrency = "3"이므로 3개의 컨슈머 스레드 생성
	 * @param records
	 */
	@KafkaListener(topics = "test", groupId = "test-group-03", concurrency = "3")
	public void concurrentBatchListener(ConsumerRecords<String,String> records){
		records.forEach(record -> log.info("record : {}", record.toString()));
	}


	/////////////////////[start] batch commit listener + batch consumer listener ////////////////////

	/**
	 * [공통] : listener.type = batch, ack-mode = manual_immediate 설정
	 * 수동 커밋을 위해 Acknowledgment을 받아서 acknowledge() 로 수동 커
	 * @param records
	 * @param ack
	 */
	@KafkaListener(topics = "test", groupId = "test-group-01")
	public void commitListener(ConsumerRecords<String,String> records, Acknowledgment ack){
		records.forEach(record -> log.info(record.toString()));
		ack.acknowledge();
	}

	/**
	 * [공통] : listener.type = batch, ack-mode = manual_immediate Or manual 설정
	 * 동기 커밋, 비동기 커밋을 하기 위해 컨슈머를 파라미터로 받
	 * @param records
	 * @param consumer
	 */
	@KafkaListener(topics = "test", groupId = "test-group-02")
	public void consumerCommitListener(ConsumerRecords<String,String> records, Consumer<String,String> consumer){
		records.forEach(record -> log.info("record : {}",record.toString()));
		consumer.commitAsync();
	}


	/////////////////////[start] custom listener factory ////////////////////

	/**
	 * 커스텀 컨테이너 팩토리 적용
	 * @param data
	 */
	@KafkaListener(topics = "test",
				   groupId = "test-group",
				   containerFactory = "customContainerFactory")
	public void customListener(String data){
		log.info("data : {}", data);
	}
}
