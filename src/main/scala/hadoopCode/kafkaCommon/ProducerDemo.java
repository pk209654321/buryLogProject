package hadoopCode.kafkaCommon;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class ProducerDemo {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("metadata.broker.list", "leader:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		for (int i = 0; i <= 1100; i++)
			if(i%2==0){
				producer.send(new KeyedMessage<String, String>("first", "wyd2"));
			}else{
				producer.send(new KeyedMessage<String, String>("first", "wyd1"));
			}

	}
}