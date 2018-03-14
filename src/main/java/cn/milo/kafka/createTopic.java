package cn.milo.kafka;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.zookeeper.ZKUtil;

import java.util.Properties;

/******************************************************
 ****** @ClassName   : createTopic.java                                            
 ****** @author      : milo ^ ^                     
 ****** @date        : 2018 03 14 9:47     
 ****** @version     : v1.0.x                      
 *******************************************************/
public class createTopic {

    private static final String ZK_CONNECT = "118.212.149.51:2181,118.212.149.56:2181,118.212.149.60:2181";
    private static final int  SESSION_TIMEOUT = 30000;
    private static final int  CONNECT_TIMEOUT = 30000;

    public static void main(String[] args) {
//        Properties props = new Properties();
////        props.put("metadata.broker.list", "118.212.149.51:9092,118.212.149.56:9092");
////        props.put("serializer.class", "kafka.serializer.StringEncoder");
////        props.put("partitioner.class", "cn.ljh.kafka.kafka_helloworld.SimplePartitioner");
////        props.put("request.required.acks", "1");

        createTopic("milo5",5,3,new Properties());
    }

    public static void createTopic(String topic , int partition , int repilca , Properties properties){
        ZkUtils zkUtil = null;
        try {
            zkUtil = ZkUtils.apply(ZK_CONNECT,SESSION_TIMEOUT,CONNECT_TIMEOUT, JaasUtils.isZkSecurityEnabled());
            if(!AdminUtils.topicExists(zkUtil,topic)){
                AdminUtils.createTopic(zkUtil,topic,partition,repilca,properties, RackAwareMode.Enforced$.MODULE$);
            }else{
                System.out.println(111);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
