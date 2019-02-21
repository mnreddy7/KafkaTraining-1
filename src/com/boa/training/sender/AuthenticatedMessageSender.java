package com.boa.training.sender;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;



public class AuthenticatedMessageSender {

	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("security.protocol", "SASL_PLAINTEXT");
		props.setProperty("sasl.mechanism","PLAIN");
		
		KafkaProducer<String,String> producer = new KafkaProducer<>(props);
		
		String topicName="my-topic";
		/*for(int i=0;i<50;i++) {
			String key = i%10 == 0 ? "message-1": i%2 == 0 ?"message-2": "message-3";
			ProducerRecord<String,String> record = new ProducerRecord<>(topicName,key,"This is test message --"+i);	
			producer.send(record);
		}*/
		for(int i=0;i<50;i++) {
		ProducerRecord<String,String> record = new ProducerRecord<>(topicName,"My Message"+i,"This is test message"+i);
		
		Future<RecordMetadata> future = producer.send(record);
		try {
			RecordMetadata rmd=future.get();
			System.out.println("message delivered to parition: "+rmd.partition()+" at offset: "+rmd.offset());
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		}
		producer.close();
		System.out.println("sent");
	}

}
