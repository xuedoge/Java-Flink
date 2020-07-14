package io.github.streamingwithflink.util;

import io.github.streamingwithflink.test.HbaseTest;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

public class SinkToHbase extends RichSinkFunction<CarBean> {


//    public HbaseTest hbaseHandle = new HbaseTest();
//
//    /**
//     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
//     *
//     * @param parameters
//     * @throws Exception
//     */
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        try {
//            hbaseHandle.initHbase();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
//
//    @Override
//    public void close() throws Exception {
//        super.close();
//        //关闭连接和释放资源
//        try {
//            hbaseHandle.closeHbase();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
//
//    /**
//     * 每条数据的插入都要调用一次 invoke() 方法
//     *
//     * @param car
//     * @param context
//     * @throws Exception
//     */
//    @Override
//    public void invoke(CarBean car, Context context) throws Exception {
//        //组装数据，执行插入操作
//        try {
//            hbaseHandle.insertData("test", car);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
    private static Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.property.clientPort", "2182");
            configuration.set("hbase.zookeeper.quorum", "localhost");
            // 集群配置↓
            // configuration.set("hbase.zookeeper.quorum", "101.236.39.141,101.236.46.114,101.236.46.113");
            configuration.set("hbase.master", "192.168.180.130:60000");
            connection = ConnectionFactory.createConnection(configuration);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        try {
            if (connection != null)
                connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param car
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(CarBean car, Context context) throws Exception {
        //组装数据，执行插入操作
        try {
            TableName tablename = TableName.valueOf("test");
            Put put = new Put(((car.idString() + ":" + car.timeString()).getBytes()));// row "car_1:155********"
            // 参数：1.列族名 2.列名 3.值
            put.addColumn("cf".getBytes(), "sum".getBytes(), car.distanceString().getBytes());
            // HTable table = new
            // HTable(initHbase().getConfiguration(),tablename);已弃用
            Table table = connection.getTable(tablename);
            table.put(put);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
