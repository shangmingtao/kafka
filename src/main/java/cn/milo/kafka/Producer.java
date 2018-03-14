package cn.milo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;

import java.util.Properties;

/******************************************************
 ****** @ClassName   : Producer.java                                            
 ****** @author      : milo ^ ^                     
 ****** @date        : 2018 03 14 11:34     
 ****** @version     : v1.0.x                      
 *******************************************************/
public class Producer {

    static Logger log = Logger.getLogger(Producer.class);

    private static final String TOPIC = "milo2";
    private static final String BROKER_LIST = "118.212.149.51:9092";
    private static KafkaProducer<String,String> producer = null;

    static {
        Properties configs = initConfig();
        producer = new KafkaProducer<String, String>(configs);
    }

    private static Properties initConfig(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKER_LIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        return properties;
    }

    public static void main(String[] args) throws InterruptedException {
        ProducerRecord<String , String> record = null;
        for (int i = 0; i < 100000; i++) {
            record = new ProducerRecord<String, String>(TOPIC,
//                    null,
//                    System.currentTimeMillis(),
//                    "key"+(int)(10*(Math.random())),
                    "value"+(int)(10*(Math.random())));
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (null != e){
                        System.out.println("send error");
                        log.info(e.getMessage());
                        e.printStackTrace();
                    }else {
                        System.out.println(String.format("offset:%s,partition:%s",recordMetadata.offset(),recordMetadata.partition()));
                    }
                }
            });

//            Thread.currentThread().sleep(2000);
        }

        producer.close();

    }
}
