package com.dilip.kafkacourse;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaProducerClient {

	public static void main(String...strings ) {

		final Logger logger = LoggerFactory.getLogger(KafkaProducerClient.class);

		String bootstrapServer = "localhost:9092";
		String topic="first_topic";

		Properties props = new Properties();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

		try (KafkaProducer<String,String> producer=new KafkaProducer<String,String>(props)){
			
			for(int i=0;i<9;i++) {
				String key = "id_"+i;
				ProducerRecord<String,String> record = new ProducerRecord<String,String>
						(topic, key, "Record "+i);
				producer.send(record,new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						// TODO Auto-generated method stub
						if(exception == null) {
						logger.info("Received meta data.\n"+
									"Topic : "+metadata.topic()+"\n"+
									"Partition : "+metadata.partition()+"\n"+
									"Offset : "+metadata.offset()+"\n"+
									"TimeStamp : "+metadata.timestamp());
						
						}else {
							logger.info("Error while producing "+exception);
						}
						
					}
					
				});
				producer.flush();
			}
		} 
		logger.info("test");
	}

}
