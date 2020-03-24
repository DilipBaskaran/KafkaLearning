package com.dilip.kafkacourse;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerClient {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Logger logger = LoggerFactory.getLogger(KafkaConsumerClient.class.getName());
		
		String bootstrapServers = "127.0.0.1:9092";
		
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-app");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(properties);
		
		consumer.subscribe(Arrays.asList("twitter_tweets"));
		
		
		while(true) {
			ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record : records) {
				logger.info("key : "+record.key()+", value = "+record.value());
				logger.info("Partition :"+record.partition()+" , Offset : "+record.offset());
			}
			
		}

	}

}
