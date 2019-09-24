package hadoopCode.kafkaCommon;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
　　* @Description: TODO kafka生产者
　　* @param
　　* @return
　　* @throws
　　* @author lenovo
　　* @date 2019/9/5 14:18
　　*/
public class ProducerDemo {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("metadata.broker.list", "nfbigdata-54:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        for (int i = 0; i <= 1000; i++){
            Thread.sleep(1000L);
            producer.send(new KeyedMessage<String, String>("buryTest", i+"_20190905_" + i) );
        }
           /* if (i % 2 == 0) {
                producer.send(new KeyedMessage<String, String>("buryTest", "wyd2" + i));
            } else {
                producer.send(new KeyedMessage<String, String>("buryTest", "wyd1" + i));
            }*/
    }
}