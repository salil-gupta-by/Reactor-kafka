package com.reactive.UpdateApp.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.web.bind.annotation.RestController;

import com.reactive.UpdateApp.util.KafkaReceiverEmp;



@RestController
public class UpdateController {
	
	private static final Logger log = LoggerFactory.getLogger(UpdateController.class);

	
	@Autowired
	KafkaReceiverEmp kafkaReceiver;
	
	
	@EventListener(ApplicationStartedEvent.class)
	public void consumeMessage() {
		
		 log.debug("Inside Kafka Listener");
		 kafkaReceiver.consumeMessages();

		
	}

	
}
