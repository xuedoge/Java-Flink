package io.github.streamingwithflink.test;
//flink 1.10.0 scala_2.11
import com.alibaba.fastjson.JSONObject;
import io.github.streamingwithflink.chapter1.AverageSensorReadings;
import io.github.streamingwithflink.kafka.KafkaWriter;
import io.github.streamingwithflink.util.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Properties;

public class TestFlink {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 非常关键，一定要设置启动检查点！！
        env.enableCheckpointing(5000);

        // use event time for the application
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // configure watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        //kafka configure
        Properties prop = new Properties();
        prop.put("bootstrap.servers", KafkaWriter.BROKER_LIST);
        prop.put("zookeeper.connect", "localhost:2181");
        prop.put("group.id", KafkaWriter.TOPIC);
        prop.put("key.serializer", KafkaWriter.KEY_SERIALIZER);
        prop.put("value.serializer", KafkaWriter.VALUE_SERIALIZER);
        prop.put("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<String>(
                KafkaWriter.TOPIC,
                new SimpleStringSchema(),
                prop
        )
                .setStartFromLatest());

//                //单线程打印，控制台不乱序，不影响结果
//                        setParallelism(1);

        //从kafka里读取数据，转换成Car对象
        DataStream<TestReading> dataStream = dataStreamSource
                .map(value -> JSONObject.parseObject(value, TestReading.class))
                .assignTimestampsAndWatermarks(new TestTimeAssigner());

        //dataStream.print();

//        // ingest sensor stream
//        DataStream<TestReading> sensorData = env
//                // SensorSource generates random temperature readings
//                .addSource(new TestSource())
//                // assign timestamps and watermarks which are required for event time
//                .assignTimestampsAndWatermarks(new TestTimeAssigner());
//        //sensorData.print();

        SingleOutputStreamOperator<Tuple3<String, Double, Long>> testData1 = dataStream
                // convert Fahrenheit to Celsius using and inlined map function
                .map( r -> new TestReading(r.id, r.timestamp, r.x, r.y ))
                .keyBy(r -> r.id)
                .timeWindow(Time.seconds(2), Time.seconds(1) )
                //timeWindow产生长度2间隔1的滑动窗口。
                .apply(new ApplyTest())
                .keyBy(0)
                .reduce((x,y)->(new Tuple3<>(x.f0, x.f1+y.f1, y.f2)));
//                .sum(1);//sum聚合存在问题，keyed流sum聚合仅仅“计算指定字段的和”。不会处理其余字段数据。
        testData1.print();
        //testData1.map(r->new CarBean(r.f0, r.f2, r.f1));

        SingleOutputStreamOperator<CarBean> dataForSink = testData1.map(r->new CarBean(r.f0, r.f2, r.f1));
        dataForSink.addSink(new SinkToHbase());


        // execute application
        env.execute("Test for Flink trans");
    }
//    public static class AggregateTest implements AggregateFunction<SensorReading, Integer, Tuple2<Integer, Integer>>{
//
//
//        @Override
//        public Integer createAccumulator() {
//            return 0;
//        }
//
//        @Override
//        public Integer add(SensorReading sensorReading, Integer integer) {
//            return integer += 1;
//        }
//
//        @Override
//        public Tuple2<Integer, Integer> getResult(Integer integer) {
//            return new Tuple2<>(integer,integer);
//        }
//
//        @Override
//        public Integer merge(Integer a, Integer b) {
//            return a+b;
//        }
//    }

    public static class ApplyTest implements WindowFunction<TestReading, Tuple3<String, Double, Long>, String, TimeWindow> {
        /**
         * apply() is invoked once for each window.
         *
         * @param carId the key (carId) of the window
         * @param timeWindow meta data for the window
         * @param input an iterable over the collected sensor readings that were assigned to the window
         * @param output a collector to emit results from the function
         */
        @Override
        public void apply(String carId, TimeWindow timeWindow, Iterable<TestReading> input, Collector<Tuple3<String, Double, Long>> output) throws Exception {

            TestReading[] post = new TestReading[2];
            post[1]=new TestReading();
            Iterator<TestReading> init = input.iterator();
            int i =0;
            while(init.hasNext()){
                post[i]=new TestReading();
                post[i]=init.next();
                i++;
            }

//            for (TestReading r : input) {
//                sum += r.x;
//            }

//            if(carId .equals("car_11")){
//                System.out.println(i);
//            }
            double dis = 0.0;
            dis = distance(post[0].x , post[0].y , post[1].x , post[1].y);//KM

//            double dis = post[0].x + post[0].y - post[1].x - post[1].y;
            if(i<2) {
                dis = 0.0;
            }

            output.collect(new Tuple3<>(carId, dis, timeWindow.getEnd()));
        }



    }
    /**
     *  User-defined WindowFunction to compute the average temperature of SensorReadings
     */

    public static double distance(double lat1, double lng1, double lat2, double lng2) {

        //转弧度
        lat1 = lat1  * Math.PI / 180;
        lat2 = lat2  * Math.PI / 180;
        lng1 = lng1  * Math.PI / 180;
        lng2 = lng2  * Math.PI / 180;

        int EARTH_R = 6371; //km

        double cos = Math.cos(lat2) * Math.cos(lat1) * Math.cos(lng2 -lng1) + Math.sin(lat1) * Math.sin(lat2);
        return EARTH_R * Math.acos(cos);
    }
}
