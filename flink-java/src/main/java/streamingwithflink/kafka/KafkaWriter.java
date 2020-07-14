package io.github.streamingwithflink.kafka;

import io.github.streamingwithflink.util.TestReading;


import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.common.Time;
import sun.util.calendar.BaseCalendar;
import com.alibaba.fastjson.JSON;

import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaWriter {
    //本地的kafka机器列表
    public static final String BROKER_LIST = "localhost:9092";
    //kafka的topic
    public static final String TOPIC = "test";
    //key序列化的方式，采用字符串的形式
    public static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    //value的序列化的方式
    public static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static final double curX[] = new double[10];
    public static final double curY[] = new double[10];


    public static void writeToKafka() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", KEY_SERIALIZER);
        props.put("value.serializer", VALUE_SERIALIZER);

        //定义kafka生产者环境
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        //source -- flink
        TestReading source;
        Random rand = new Random();

        for (int i = 0; i < 10; i++) {
//            String carId = "car_" + i ;
//            Long time = Calendar.getInstance().getTimeInMillis();
//            double x = rand.nextDouble() * 360.0 - 180.0;
//            double y = rand.nextDouble() * 360.0 - 180.0;
            String carId = "car_" + i;
            Long time = Calendar.getInstance().getTimeInMillis();
            double x = curX[i] + rand.nextDouble();
            double y = curY[i] + rand.nextDouble();

            source = new TestReading(carId, time, x, y);

//        person.setName("hqs" + randomInt);
//        person.setAge(randomInt);
//        person.setCreateDate(new Date());
//
            //转换成JSON
            String sourceJson = JSON.toJSONString(source);

            //包装成kafka发送的记录
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, null,
                    null, sourceJson);
            //发送到缓存
            producer.send(record);
            System.out.println("向kafka发送数据:" + sourceJson);
            //立即发送
            producer.flush();
        }

    }

    public static void main(String[] args) {
        Random rand = new Random();
        for (int i = 0; i < 10; i++) {
            // -180 < x,y < 180
            curX[i] = rand.nextDouble() * 360.0 - 180.0;
            curY[i] = rand.nextDouble() * 360.0 - 180.0;
        }
        while (true) {
            try {
                //每秒写一条数据
                TimeUnit.SECONDS.sleep(1);
                writeToKafka();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
