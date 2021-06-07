package com.reactive.UpdateApp.util;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.reactive.UpdateApp.model.Employee;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;


@Component
public class KafkaReceiverEmp {
	
	@Autowired
	KafkaProducerEmp kafkaProducerEmp;
	
	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String TOPIC_NAME= "app_updates";
	
	
	private static final Logger log = LoggerFactory.getLogger(KafkaProducerEmp.class);

	

	public Disposable consumeMessages() {
		
		Map<String,Object> consumerProps = new HashMap<>();
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "groupid");
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");
		
		
		ReceiverOptions<Integer, String> receiverOptions = ReceiverOptions.create(consumerProps);
		
		receiverOptions.subscription(Collections.singleton(TOPIC_NAME));
		
		
		KafkaReceiver<Integer, String> receiver = KafkaReceiver.create(receiverOptions);
		
		Flux<ReceiverRecord<Integer, String>> inboundFlux = receiver.receive();
		
		log.debug(" Consumed data " + inboundFlux.toString());
		
		return inboundFlux.subscribe(record -> {
				ReceiverOffset offset = record.receiverOffset();
				log.debug("Received message: topic-partition=%s offset=%d timestamp=%s key=%d value=%s\n",
                offset.topicPartition(),
                offset.offset(),
                dateFormat.format(new Date(record.timestamp())),
                record.key(),
                record.value());
        offset.acknowledge();
        
        Employee emp= new Employee();
        
        boolean isValid= validateEmployee(record.value(),emp);
        
			if (isValid) {
				kafkaProducerEmp.sendMessages(record.value(), "employee_updates");
				
				log.debug("employee details has been send to kafka employee_updates successfully");
				
			} else {
				kafkaProducerEmp.sendMessages(record.value(), "employee_DLQ");
				
				log.debug("employee details has been send to kafka employee_DLQ successfully");
			}		
			
	});
	
	
	}
	
	
	public boolean validateEmployee(String ConsumedValue, Employee emp) {
		
		boolean flag = true;
		log.debug("Validating JSON consumed " + ConsumedValue);
		
        ObjectMapper mapper= new ObjectMapper();
        
        try {
			emp= mapper.readValue(ConsumedValue, Employee.class);
		} catch (JsonMappingException e) {
			flag = false;
			log.debug("Validation Failed " +e);
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			flag = false;
			log.debug("Validation Failed " +e);
			e.printStackTrace();
		}
		
		
		return flag;
	}
}
