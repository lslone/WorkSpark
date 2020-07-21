package com.corey.streamingframework.Utils;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Properties;

public class KafkaUtil implements Serializable {
    public static Log monitorLog = LogFactory.getLog(KafkaUtil.class);

    private static Producer<String,String> producer = null;

    private static KafkaProducer<String,String> authProduce = null;

    public synchronized static Producer<String,String> getProducer(String outBrokers){
        if(null == producer){
            Properties props = new Properties();
            props.put("metadata.brokers.list",outBrokers);
            props.put("serializer.class","kafka.serializer.StringEncoder");
            props.put("request.required.acks","-1");
            ProducerConfig config = new ProducerConfig(props);
            producer = new Producer<String, String>(config);
        }
        return producer;
    }

    public synchronized static KafkaProducer<String,String> getAuthProduce(String outBrokers,String username,String password){
        if(null == authProduce){
            Properties props = new Properties();
            props.put("bootstrap.servers",outBrokers);
            props.put("key.serializer",StringSerializer.class);
            props.put("value.serializer",StringSerializer.class);
            props.put("request.required.acks","-1");
            if((StringUtils.isNotBlank(username)) && StringUtils.isNotBlank(password)){
                props.put("security.protocol","SASL_PLAINTEXT");
                props.put("sasl.mechanism","PLAIN");
                String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{0}\" password=\"{1}\";";
                props.put("sasl.jaas.config",MessageFormat.format(jaasTemplate,username,password));
            }
            authProduce = new KafkaProducer<String, String>(props);
        }
        return authProduce;
    }


    @Deprecated
    public static void sendMsgToKafka(String outBrokers,String outTopic,String[] msg) throws Exception{
        long start = System.currentTimeMillis();
        Producer<String, String> producer = getProducer(outBrokers);
        ArrayList<KeyedMessage<String, String>> msgList = new ArrayList<>();
        for (String aMsg : msg) {
            String key = Math.round(Math.random() * 100000) + "";
            msgList.add(new KeyedMessage<String, String>(outTopic,key,aMsg));
            monitorLog.debug("发送消息： " + aMsg);
        }
        producer.send(msgList);
        long end = System.currentTimeMillis();
        monitorLog.debug("send " + msg.length + " msg to kafka total cost: " + (end -start) + " ms");
    }

    public static void sendMsgToAuthorizeKafka(String outBrokers,String outTopic,String username,String password,String[] msg) throws Exception{
        long start = System.currentTimeMillis();
        KafkaProducer<String, String> producer = getAuthProduce(outBrokers, username, password);
        ArrayList<KeyedMessage<String, String>> msgList = new ArrayList<>();
        for (String aMsg : msg) {
            String key = Math.round(Math.random() * 100000) + "";
            ProducerRecord<String, String> record = new ProducerRecord<>(outTopic, key, aMsg);
            producer.send(record);
            monitorLog.debug("发送消息： " + aMsg);
        }
        producer.flush();
        long end = System.currentTimeMillis();
        monitorLog.debug("send " + msg.length + " msg to kafka total cost: " + (end -start) + " ms");
    }

    public static void sendMsgToKafka(String outBrokers,String outTopic,String username,String password,String[] msg) throws Exception{
        sendMsgToAuthorizeKafka(outBrokers,outTopic,username,password,msg);
    }
    /*

     */
    public static String getRandomString(int length){
        String key = "dsjfknajfnds";
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            sb.append(key.charAt((int)Math.round(Math.random() *61)));
        }
        return sb.toString();
    }

}
