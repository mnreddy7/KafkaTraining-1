package com.boa.training.custompartitioner;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class MessagePartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> arg0) {
		
	}

	@Override
	public void close() {
		
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object obj, byte[] valueBytes, Cluster cluster) {
		String msgKey = (String) key;
		int partition = 3;
		if(msgKey.equals("message-1")) {
			partition=0;
		}else if(msgKey.equals("message-2")) {
			partition=1;
		}else if(msgKey.equals("message-3")) {
			partition=2;
		}
		return partition;
	}

}
