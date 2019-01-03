package io.schneezey.producer;

import java.net.InetAddress;
//import java.util.Calendar;
import java.util.Properties;

import org.apache.avro.Schema;

import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.schneezey.pojo.GenericSpringMapper;
import io.schneezey.pojo.TestPojo;

/**
 * Hello world!
 *
 */
public class Producer 
{
    public static void main( String[] args ) {
    	
    	Schema schema = ReflectData.get().getSchema(io.schneezey.pojo.TestPojo.class);
        System.out.println( schema.toString(true) );
        
    	produceSpringMappedPojo();
    }

    private static TestPojo createTestPojo() {
    	TestPojo pojo = new TestPojo();
    	
    	pojo.setId(Random.randomInt(999999999));
	
    	pojo.setTestBoolean(Random.randomBoolean());
    	//pojo.setTestBytes(Random.randomByte(Random.randomInt(128)));
    	//Calendar.getInstance().set(1910 + Random.randomInt(85), Random.randomInt(12), Random.randomInt(28));
    	//pojo.setTestDate(Calendar.getInstance().getTime());
    	pojo.setTestDouble(Random.randomDouble());
    	pojo.setTestFloat(Random.randomFloat());
    	pojo.setTestInt(Random.randomInt());
    	pojo.setTestLong(Random.randomLong());

    	pojo.setTestN1Boolean(Random.randomBoolean());
    	//pojo.setTestNBytes(Random.randomByte(Random.randomInt(128)));
    	//Calendar.getInstance().set(1910 + Random.randomInt(85), Random.randomInt(12), Random.randomInt(28));
    	//pojo.setTestN1Date(Calendar.getInstance().getTime());
    	pojo.setTestN1Double(Random.randomDouble());
    	pojo.setTestN1Float(Random.randomFloat());
    	pojo.setTestN1Int(Random.randomInt());
    	pojo.setTestN1Long(Random.randomLong());
/*
    	{
    		int value = Random.randomInt(3);
    		switch (value) {
    		case 1:
    			pojo.setTestenum(TestPojo.TEST_ENUM.TYPEA);
    			break;
    		case 2:
    			pojo.setTestenum(TestPojo.TEST_ENUM.TYPEB);
    			break;
    		case 3:
    		default:
    			pojo.setTestenum(TestPojo.TEST_ENUM.TYPEC);
    			break;
    		}
    	}
*/
    	return pojo;
    }
    
	private static void produceSpringMappedPojo() {
		KafkaProducer<String,Object> producer = null;
		GenericSpringMapper<TestPojo> mapper = new GenericSpringMapper<TestPojo>();
    	try {
	        Properties config = new Properties();
	        config.put("client.id", InetAddress.getLocalHost().getHostName());
	        config.put("bootstrap.servers", "localhost:9092");
	        config.put("acks", "all");
	        
	        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
	                org.apache.kafka.common.serialization.StringSerializer.class);
	        
	        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
	        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

	        producer = new KafkaProducer<String, Object> (config);
	        
	        
	        for (int i = 0; i < 3; i++) {
	        	TestPojo pojo = createTestPojo();
		        ProducerRecord<String,Object> avroRecord = new ProducerRecord<String,Object> ("test.pojo.springmapper.generic.confluent.avro", pojo.getId().toString(), mapper.mapObjectToRecord(pojo));
	        	producer.send( avroRecord );
		        System.out.println("Avro Message produced" + pojo.toString());
			}
	        producer.close();
    	} catch (Exception ee) {
    		ee.printStackTrace();
    		System.out.println("Exception Caught \n" + ee.getLocalizedMessage());
    	}
    	finally {
    		if (producer != null) producer.close();
    	}
	}

}
