package com.boa.training.sender;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;



public class MessageSender {

	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaProducer<String,String> producer = new KafkaProducer<>(props);
		
		String topicName="my-topic";
		/*for(int i=0;i<50;i++) {
			String key = i%10 == 0 ? "message-1": i%2 == 0 ?"message-2": "message-3";
			ProducerRecord<String,String> record = new ProducerRecord<>(topicName,key,"This is test message --"+i);	
			producer.send(record);
		}*/
		ProducerRecord<String,String> record = new ProducerRecord<>(topicName,"My Message","This is test message");
		
		Future<RecordMetadata> future = producer.send(record);
		try {
			RecordMetadata rmd=future.get();
			System.out.println("message delivered to parition: "+rmd.partition()+" at offset: "+rmd.offset());
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		producer.close();
		System.out.println("sent");
	}

}
