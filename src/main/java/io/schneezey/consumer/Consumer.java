package io.schneezey.consumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.schneezey.pojo.GenericSpringMapper;
import io.schneezey.pojo.TestPojo;


@SpringBootApplication
public class Consumer implements CommandLineRunner
{
    public static Logger logger = LoggerFactory.getLogger(Consumer.class);
    private static GenericSpringMapper<TestPojo> mapper = null;

    public static void main( String[] args )
    {
    	mapper = new GenericSpringMapper<TestPojo>();
    	SpringApplication.run(Consumer.class, args).close();
    }
    
    @Override
    public void run(String... args) throws Exception {
    	ContainerProperties containerProps = new ContainerProperties("test.pojo.springmapper.generic.confluent.avro");
    	
    	containerProps.setMessageListener(new MessageListener<String,GenericRecord>() {
    		@Override
    		public void onMessage(ConsumerRecord<String,GenericRecord> message) {
    			logger.info("received:  " + message.key());
  
    			TestPojo target = new TestPojo();
    			try {
    				logger.info("recieving TestPojo");
					mapper.mapRecordToObject(message.value(), target);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					logger.error(e.getMessage());
				}
    			logger.info(target.toString());
    			return;
    		}
    	});
        
		Map<String, Object> consumerProps = new HashMap<String, Object>();
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
		consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
		consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
		consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
		//props.put( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        
		DefaultKafkaConsumerFactory<String, GenericRecord> cf = new DefaultKafkaConsumerFactory<String, GenericRecord>(consumerProps);
		KafkaMessageListenerContainer<String, GenericRecord> container = new KafkaMessageListenerContainer<String, GenericRecord>(cf,
				containerProps);
		
        container.setBeanName("topicContainerBean");
        container.start();
        logger.info("started.");
    }

}
