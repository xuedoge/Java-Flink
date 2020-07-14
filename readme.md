# 基于Flink的GPS数据实时处理

## 需求
数据源是一个实时的车辆GPS位置信息的数据流，该数据流包含4个字段（车牌号，经度，维度，时间戳），每辆车每10秒中上报一条GPS数据，总共有上万辆车都会给本系统上报GPS数据。用Flink从Kafka消息该数据流，实时计算出当天0点到当前每辆车的行驶里程（每天0点清零），计算结果写入到HBase中。

Flink不能从HBase读取数据。提供一个页面，输入车牌号，可以查询到这辆车当天0点到当前的行驶里程数，并展示一条以时间为横轴、里程为纵轴的一条变化曲线。由于网络环境的复杂性，要考虑GPS数据可能乱序，比如9点10分20秒的数据可能比9点10分10秒的数据晚到。
>  该项目使用Kafka做数据源，HBASE做数据汇，Flink进行实时流计算。数据是实时生成到Kafka中的，模拟实际生产环境。Flink长时间运行，从Kafka中获取数据进行运算，并将运算结果存入HBASE中。
### 项目环境
- Kafka分布式消息系统，该项目中配置了单机环境。
- Flink分布式实时流计算框架，该项目中配置了单机环境。
- HBASE分布式非关系型数据库，该项目中配置了单机环境。

### 数据生成：
- KafkaWriter类。
- 为了模拟生产环境，用代码实时生成数据存到Kafka中。
- 模拟了十辆汽车，每秒钟随机产生一组数据。数据为汽车的GPS位置信息。
- 运行KafkaWriter，首先随机生成十辆汽车的初始位置。然后程序持续运行，每秒钟会对这十个随机的位置进行一定幅度的微小变化，模拟汽车在行驶。


### Flink实时运算：
- TestFlink类。
- 设定检查点，设定水位线，提取Kafka的数据，对数据设定水位线、规定时间戳。
- 对数据流进行转换。
- **map**（转换成Reading类），**keyBy**（按照车id分类），**timeWindow**（划分滑动窗口，每个窗口存两个相邻的GPS数据），**apply**（对一辆车的两个相邻的GPS数据进行计算得出行驶距离），**reduce**（聚合车辆的行驶距离结果）
- 最后将数据Sink到HBASE。
<pre>
SingleOutputStreamOperator<Tuple3<String, Double, Long>> testData1 = dataStream
                .map( r -> new TestReading(r.id, r.timestamp, r.x, r.y ))
                .keyBy(r -> r.id)
                .timeWindow(Time.seconds(2), Time.seconds(1) )
                //timeWindow产生长度2间隔1的滑动窗口。
                .apply(new ApplyTest())
                .keyBy(0)
                .reduce((x,y)->(new Tuple3<>(x.f0, x.f1+y.f1, y.f2)));
//                .sum(1);//sum聚合存在问题，keyed流sum聚合仅仅“计算指定字段的和”。不会处理其余字段数据。
</pre>

### HBASE：
- HBASE是一个分布式的非关系型数据库。
- HBASE需要特殊的设计，因为要实时查询到每天零点到当前时间点的数据，所以项目中将HBASE的RowKey设计成了车辆ID+时间戳的行驶（carId + ":" + startTime）。例如car_1:1589741765000。
- 因为HBASE中的RowKey是按照字典序排序的，所以一辆车不同时间存储的数据就会按顺序存起来，查询的时候因为有序所以查询效率很高。
### 图像展示
- 使用了JFreeChart展示动态变化的折线图。
