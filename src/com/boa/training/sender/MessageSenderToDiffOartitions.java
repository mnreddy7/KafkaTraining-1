package com.boa.training.sender;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;



public class MessageSenderToDiffOartitions {

	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("partitioner.class", "com.boa.training.custompartitioner.MessagePartitioner");
		
		KafkaProducer<String,String> producer = new KafkaProducer<>(props);
		
		String topicName="new-topic";
		for(int i=0;i<=3;i++) {
			ProducerRecord<String,String> record = new ProducerRecord<>(topicName,"message-1","This is test message --"+i);	
			producer.send(record);
		}
		for(int i=0;i<=6;i++) {
			ProducerRecord<String,String> record = new ProducerRecord<>(topicName,"message-2","This is test message --"+i);	
			producer.send(record);
		}
		for(int i=0;i<=8;i++) {
			ProducerRecord<String,String> record = new ProducerRecord<>(topicName,"message-3","This is test message --"+i);	
			producer.send(record);
		}
		for(int i=0;i<=10;i++) {
			ProducerRecord<String,String> record = new ProducerRecord<>(topicName,"message-x","This is test message --"+i);	
			producer.send(record);
		}
		
		producer.close();
		System.out.println("sent");
	}

}
