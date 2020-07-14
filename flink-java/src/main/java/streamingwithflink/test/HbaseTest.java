package io.github.streamingwithflink.test;

import io.github.streamingwithflink.util.CarBean;
import org.apache.avro.generic.GenericData;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HbaseTest {
    private static Admin admin;
    private static Connection connection;

    public HbaseTest() throws IOException {
        connection = initHbase();
    }

    public static void main(String[] args) {
        try {
            connection = initHbase();
//            //建表
////            createTable("am_file_data", new String[] { "upload_date", "upload_user"
////                    , "upload_status", "is_processed", "file_path", "file_name", "content"});
//
//            createTable("user_table", new String[] { "information", "contact" });
//
//            User user = new User("001", "xiaoming", "123456", "man", "20", "13355550021", "1232821@csdn.com");
//            insertData("user_table", user);
//            User user2 = new User("002", "xiaohong", "654321", "female", "18", "18757912212", "214214@csdn.com");
//            insertData("user_table", user2);
//
//            List<User> list = getAllData("user_table");
//            System.out.println("--------------------插入两条数据后--------------------");
//            for (User user3 : list) {
//                System.out.println(user3.toString());
//            }
//            System.out.println("--------------------获取原始数据-----------------------");
//            getNoDealData("user_table");
//            System.out.println("--------------------根据rowKey查询--------------------");
//            User user4 = getDataByRowKey("user_table", "user-001");
//            System.out.println(user4.toString());
//            System.out.println("--------------------获取指定单条数据-------------------");
//            String user_phone = getCellData("user_table", "user-001", "contact", "phone");
//            System.out.println(user_phone);
//            User user5 = new User("test-003", "xiaoguang", "789012", "man", "22", "12312132214", "856832@csdn.com");
//            insertData("user_table", user5);
//            List<User> list2 = getAllData("user_table");
//            System.out.println("--------------------插入测试数据后--------------------");
//            for (User user6 : list2) {
//                System.out.println(user6.toString());
//            }
//            deleteByRowKey("user_table", "user-test-003");
//            List<User> list3 = getAllData("user_table");
//            System.out.println("--------------------删除测试数据后--------------------");
//            for (User user7 : list3) {
//                System.out.println(user7.toString());
//            }

//            initHbase();
//            CarBean carInfo = new CarBean("test_car_0", 1555555555, 12.1);
//            insertData("test", carInfo);
//            closeHbase();
            //List<CarBean> car = getDataByRowKey("test", "row2", 100, 200);
            String startTime = "1589741765000";
            String stopTime = "1589741785000";
            List<CarBean> car = getDataByRowKey("test", "car_1", Long.valueOf(startTime), Long.valueOf(stopTime));
            Iterator<CarBean> carIterator = car.iterator();
            while(carIterator.hasNext())
            {
                System.out.println(carIterator.next().distance);
            }



        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 连接集群
    public static Connection initHbase() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2182");
        configuration.set("hbase.zookeeper.quorum", "localhost");
        // 集群配置↓
        // configuration.set("hbase.zookeeper.quorum", "101.236.39.141,101.236.46.114,101.236.46.113");
        configuration.set("hbase.master", "192.168.180.130:60000");
        connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    public static void closeHbase() throws IOException {

        try {
            if (connection != null)
                connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * insert Data to table
     *
     * @param tableName
     * @param car
     * @throws IOException
     */
    public static void insertData(String tableName, CarBean car) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        Put put = new Put(((car.idString() + ":" + car.timeString()).getBytes()));// row "car_1:155********"
        // 参数：1.列族名 2.列名 3.值
        put.addColumn("cf".getBytes(), "sum".getBytes(), car.distanceString().getBytes());
        // HTable table = new
        // HTable(initHbase().getConfiguration(),tablename);已弃用
        Table table = initHbase().getTable(tablename);
        table.put(put);
    }

    // 获取原始数据
    public static void getNoDealData(String tableName) {
        try {
            Table table = initHbase().getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner resutScanner = table.getScanner(scan);
            for (Result result : resutScanner) {
                System.out.println("scan:  " + result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据rowKey进行范围查询
     *
     * @param tableName
     * @param carId
     * @param startTime
     * @param stopTime
     * @return
     * @throws IOException
     */
    public static List<CarBean> getDataByRowKey(String tableName, String carId, long startTime, long stopTime) throws IOException {

        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        scan.setStartRow(Bytes.toBytes(carId + ":" + startTime));
        scan.setStopRow(Bytes.toBytes(carId + ":" + stopTime));


        List<CarBean> carDisList = new ArrayList<CarBean>();

        // 先判断是否有此条数据
        if (!scan.isGetScan()) {
            ResultScanner scanner = table.getScanner(scan);
            for (Result r : scanner) {
                for (Cell cell : r.rawCells()) {
                    CarBean car = new CarBean();
                    String rowKey = Bytes.toString(r.getRow());
                    double dis = Double.valueOf(Bytes.toString(CellUtil.cloneValue(cell)));
                    String id = rowKey.split(":")[0];
                    long time = Long.valueOf(rowKey.split(":")[1]);
                    //System.out.println(dis+" "+id+ " "+time);

                    car.distance = dis;
                    car.id = id;
                    car.timestamp = time;
                    carDisList.add(car);
                }
            }
            scanner.close();
            return carDisList;
        }
        else return null;

    }


    /**************************************USELESS**********************************************************/

    // 创建表
    public static void createTable(String tableNmae, String[] cols) throws IOException {

        TableName tableName = TableName.valueOf(tableNmae);
        admin = initHbase().getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("表已存在！");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
    }

    // 查询指定单cell内容
    public static String getCellData(String tableName, String rowKey, String family, String col) {

        try {
            Table table = initHbase().getTable(TableName.valueOf(tableName));
            String result = null;
            Get get = new Get(rowKey.getBytes());
            if (!get.isCheckExistenceOnly()) {
                get.addColumn(Bytes.toBytes(family), Bytes.toBytes(col));
                Result res = table.get(get);
                byte[] resByte = res.getValue(Bytes.toBytes(family), Bytes.toBytes(col));
                return result = Bytes.toString(resByte);
            } else {
                return result = "查询结果不存在";
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "出现异常";
    }

    // 删除指定cell数据
    public static void deleteByRowKey(String tableName, String rowKey) throws IOException {

        Table table = initHbase().getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 删除指定列
        // delete.addColumns(Bytes.toBytes("contact"), Bytes.toBytes("email"));
        table.delete(delete);
    }

    // 删除表
    public static void deleteTable(String tableName) {

        try {
            TableName tablename = TableName.valueOf(tableName);
            admin = initHbase().getAdmin();
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
