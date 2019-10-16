package hadoopCode.kafkaCommon;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * 　　* @Description: TODO kafka生产者
 * 　　* @param
 * 　　* @return
 * 　　* @throws
 * 　　* @author lenovo
 * 　　* @date 2019/9/5 14:18
 *
 */
public class ProducerDemo {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("metadata.broker.list", "nfbigdata-54:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        /*for (int i = 0; i <= 1000; i++){
            Thread.sleep(1000L);
            producer.send(new KeyedMessage<String, String>("buryTest", i+"_20190905_" + i) );
        }*/
        FileReader fileReader = new FileReader("C:\\Users\\lenovo\\Desktop\\upload\\anios.txt");
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            System.out.println(line);
            producer.send(new KeyedMessage<String, String>("buryTest", line));
        }
        bufferedReader.close();
    }
}