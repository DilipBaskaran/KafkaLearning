package com.dilip.kafkacourse;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

	public static void main(String... a) {


		new TwitterProducer().run();
	}

	private void run() {

		logger.info("Twitter Setup.........");
		
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

		Client client = createTwitterClient(msgQueue);
		client.connect();

		KafkaProducer<String,String> producer = createKafkaProducer();

		Runtime.getRuntime().addShutdownHook(new Thread(()->{
			logger.info("stopping the application");
			logger.info("shutting down client from twitter.....");
			client.stop();
			logger.info("closing producer.....");
			producer.close();
			logger.info("Done!.....");
		}));
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5,TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				client.stop();
			}

			if( msg != null ) {
				logger.info(msg);
				producer.send(new ProducerRecord<String,String>("twitter_tweets",null,msg),new Callback() {
					
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						// TODO Auto-generated method stub
						if(exception!=null) {
							logger.error("Something bad happened",exception);
						}
					}
				});
			}
		}

		logger.info("End Of Application");
	}

	private static final String consumerKey="qWXbOalekYYAd3UWQpDkjsLIf";
	private static final String consumerSecret="mBP7uIJdWLodj1Yg3P2SNTeZPLhrODMqxn9DquvlpkacwxRzQo";
	private static final String token="245126432-OtekJd0ODNYzgW8DhByNOGNPfVZvonMxAEPUKZRw";
	private static final String secret="Jpr11hwpHnPUC9JVSQ2AaRP5Bbu5i9e54yDozo4lv4iIc";
	private static final List<String> terms = Lists.newArrayList("Corona","CoronaVirus");
	
	private static Client createTwitterClient(BlockingQueue<String> msgQueue) {


		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")                              // optional: mainly for the logs
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		return builder.build();


	}
	
	public KafkaProducer<String,String> createKafkaProducer(){
		String bootstrapServer = "localhost:9092";
		String topic="first_topic";

		Properties props = new Properties();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
		
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		
		props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
		
		return new KafkaProducer<String,String>(props);
	}

}
