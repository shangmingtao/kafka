package cn.milo.kafka;

import kafka.tools.ConsoleConsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.awt.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/******************************************************
 ****** @ClassName   : Consumer.java                                            
 ****** @author      : milo ^ ^                     
 ****** @date        : 2018 03 14 15:50     
 ****** @version     : v1.0.x                      
 *******************************************************/
public class Consumer {

    static Logger log = Logger.getLogger(Producer.class);

    private static final String TOPIC = "test1";
    private static final String BROKER_LIST = "118.212.149.51:9092";
    private static KafkaConsumer<String,String> consumer = null;

    static {
        Properties configs = initConfig();
        consumer = new KafkaConsumer<String, String>(configs);
    }

    private static Properties initConfig(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers","118.212.149.51:9092");
        properties.put("group.id","0");
//        properties.put("client.id","client1");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }


    public static void main(String[] args) {
        consumer.subscribe(Arrays.asList("milo2"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                consumer.commitSync();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                long commitedOffset = 0;
                for (TopicPartition topicPartition: collection) {
                    commitedOffset = consumer.committed(topicPartition).offset();
                    consumer.seek(topicPartition,commitedOffset+1);
                }
            }
        });

//        ConsumerRecords<String, String> result = consumer.poll(10);
//        log.info("result = " + result.toString());
//
//        consumer.close();
//        consumer.subscribe(Arrays.asList("milo2"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10);
//            System.out.println(records.count());
            for (ConsumerRecord<String, String> record : records) {
                log.info(record);
            }
        }
    }
}
