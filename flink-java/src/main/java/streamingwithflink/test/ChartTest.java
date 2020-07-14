package io.github.streamingwithflink.test;

//RealTimeChart .java

import io.github.streamingwithflink.util.CarBean;
import org.apache.flink.api.java.tuple.Tuple2;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.XYPlot;

import org.jfree.data.xy.XYDataItem;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
//Test.java
import java.awt.BorderLayout;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import javax.swing.*;

public class ChartTest extends ChartPanel implements Runnable {
    //    private static TimeSeries timeSeries;
    private static XYSeries xySeries;
    private long value = 0;
    private static String carId = "car_1";
    private static HbaseTest base;
    private static List<CarBean> car;
    private static Iterator<CarBean> carIterator;
    private static String startTime = "1589741765000";
    private static String stopTime = "1589741785000";
    private static String lastTime = "";


    public ChartTest(String chartContent, String title, String yaxisName) throws IOException {
        super(createChart(chartContent, title, yaxisName));

        base.initHbase();
    }

    private static JFreeChart createChart(String chartContent, String title, String yaxisName) {
        //创建时序图对象
//        timeSeries = new TimeSeries(chartContent);
        xySeries = new XYSeries(chartContent);
        while (carIterator.hasNext()) {
            CarBean data = carIterator.next();
            xySeries.add(data.timestamp, data.distance);
            lastTime = "" + data.timestamp;
        }

        XYSeriesCollection xySeriesCollection = new XYSeriesCollection(xySeries);
//        TimeSeriesCollection timeseriescollection = new TimeSeriesCollection(timeSeries);

        JFreeChart jfreechart = ChartFactory.createXYLineChart(title, "TIME(10s)", yaxisName, xySeriesCollection);
        XYPlot xyplot = jfreechart.getXYPlot();
        //纵坐标设定
        ValueAxis valueaxis = xyplot.getDomainAxis();
        //自动设置数据轴数据范围
        valueaxis.setAutoRange(true);
        //数据轴固定数据范围 30s
        //valueaxis.setFixedAutoRange(30000D);

        return jfreechart;
    }

    public void run() {

        while (true) {
            try {
                CarBean data = getData();
                if (data != null) {
                    xySeries.add(data.timestamp, data.distance);
                    lastTime = "" + data.timestamp;
                    Thread.sleep(100);
                }

            } catch (InterruptedException | IOException e) {
            }
        }
    }

    private static long randomNum() {
        System.out.println((Math.random() * 20 + 80));
        return (long) (Math.random() * 20 + 80);
    }

    private static CarBean getData() throws IOException {
        stopTime = "" + Calendar.getInstance().getTimeInMillis();
        List<CarBean> carData = base.getDataByRowKey("test", carId, Long.valueOf(lastTime) + 1, Long.valueOf(stopTime));
        System.out.println(carData.size());
        CarBean data = null;
        if(carData.iterator().hasNext()){
            data = carData.iterator().next();
        }
        return data;
    }
    /**
     * @Author:whf
     * @param:
     * @Description: 获得“今天”零点时间戳 获得2点的加上2个小时的毫秒数就行
     * @Date:2018/4/12 0012
     * java自带获得当前毫秒时间戳的方法是System.currentTimeMillis()，零点是24小时轮回的零界点。所以我们把当前时间戳取24小时毫秒数取余，然后用当前毫秒时间戳减这个余就行。
     */
    public static Long getTodayZeroPointTimestamps(){
        Long currentTimestamps=System.currentTimeMillis();
        Long oneDayTimestamps= Long.valueOf(60*60*24*1000);
        return currentTimestamps-(currentTimestamps+60*60*8*1000)%oneDayTimestamps;
    }


    public static void main(String[] args) throws IOException {

        base = new HbaseTest();


        startTime = "" + getTodayZeroPointTimestamps();
        stopTime = "" + Calendar.getInstance().getTimeInMillis();

        JFrame frame = new JFrame("Car Chart");
        JPanel jp = new JPanel();
        car = base.getDataByRowKey("test", carId, Long.valueOf(startTime), Long.valueOf(stopTime));
        carIterator = car.iterator();

        ChartTest realTimeChart = new ChartTest("DISTANCE", carId, "KM");

        jp.add(realTimeChart);

        frame.getContentPane().add(jp, new BorderLayout().CENTER);
        frame.pack();
        frame.setVisible(true);

        (new Thread(realTimeChart)).start();
        frame.addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent windowevent) {
                System.exit(0);
            }

        });
    }
}



