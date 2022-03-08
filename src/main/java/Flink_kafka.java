import bean.Config;
import bean.DetailBean;
import bean.Dws_n_st_basic;
import bean.UvCount;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import scala.collection.CustomParallelizable;
import scala.collection.mutable.HashSet;
import util.DimUtil;

import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class Flink_kafka {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        executionEnvironment.setParallelism(10);

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "bg-kf01:9092,bg-kf02:9092,bg-kf03:9092"); // kafka地址
        prop.setProperty("group.id", "Flink-kafka");
        prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//      prop.setProperty("auto.offset.reset","latest");

//        DataStreamSource<Row> configInput = executionEnvironment.createInput(JdbcInputFormat.buildJdbcInputFormat()
//                .setDrivername("com.mysql.cj.jdbc.Driver")
//                .setDBUrl("jdbc:mysql://192.168.0.135:3306/bgd?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8&useSSL=false")
//                .setUsername("root")
//                .setPassword("SH021.bg")
//                .setQuery("select proid,mmac from flinkConfig")
//                .setRowTypeInfo(new RowTypeInfo(
//                        BasicTypeInfo.INT_TYPE_INFO,
//                        BasicTypeInfo.STRING_TYPE_INFO
//                ))
//                .finish());

//        DebeziumSourceFunction<String> sourceMySQL = MySqlSource.<String>builder()
//                .hostname("192.168.0.135")
//                .port(3306)
//                .username("root")
//                .password("SH021.bg")
//                .databaseList("bgd")
//                .deserializer(new StringDebeziumDeserializationSchema())
//                .startupOptions(StartupOptions.initial())
//                .tableList("flinkConfig")
//                .build();
//        DataStreamSource<String> stringDataStreamSource = executionEnvironment.addSource(sourceMySQL);
//        stringDataStreamSource.print();

//        SingleOutputStreamOperator<Config> configBean = configInput.map(new MapFunction<Row, Config>() {
//            @Override
//            public Config map(Row row) throws Exception {
//                return new Config(row.getField(0).toString(), row.getField(1));
//            }
//        });

//        configBean.print();
//
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
        FlinkKafkaConsumer<String> data_detail1 = new FlinkKafkaConsumer<>("data_detail", new SimpleStringSchema(), prop);
        data_detail1.setStartFromLatest();
        // 消费消息的topic
        DataStreamSource<String> data_detail = executionEnvironment.addSource(data_detail1);


        SingleOutputStreamOperator<JSONObject> jsonObjDetail = data_detail.map(JSON::parseObject);

//        SingleOutputStreamOperator<JSONObject> filter = jsonObjDetail.filter(new RichFilterFunction<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject jsonObject) throws Exception {
//                String mmac = jsonObject.getString("MMAC");
//                String mac = jsonObject.getString("MAC");
//                if (mmac.equals("14:6b:9c:f3:ed:27") && mac.equals("bjyNgArNtkbNp7bNpkbNtjl=")) {
//                    return true;
//                } else {
//                    return false;
//                }
//            }
//        });

        SingleOutputStreamOperator<DetailBean> detailBean = jsonObjDetail.map(jsonObject -> JSONObject.parseObject(jsonObject.toString(), DetailBean.class));

//        detailBean.print();


        SingleOutputStreamOperator<Dws_n_st_basic> Dws_n_st_basicBean = detailBean.map(new DimUtil());
//        Dws_n_st_basicBean.print();


//
//        DataStream<Dws_n_st_basic> apply1 = detailBean.join(configBean)
//                .where(detailBean1 -> detailBean1.getMmac())
//                .equalTo(config -> config.getMmac())
//                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(2000), Time.milliseconds(1000)))
//                .apply(new JoinFunction<DetailBean, Config, Dws_n_st_basic>() {
//                    @Override
//                    public Dws_n_st_basic join(DetailBean detailBean, Config config) throws Exception {
//                        return new Dws_n_st_basic(detailBean.getDataid(),
//                                detailBean.getRanges(),
//                                detailBean.getRssi0(),
//                                detailBean.getDtime(),
//                                detailBean.getIn_time(),
//                                detailBean.getMmac(),
//                                detailBean.getRid(),
//                                detailBean.getMac(),
//                                config.getProid());
//                    }
//                });
//
//        apply1.print();

//        Dws_n_st_basic(detailBean.getDataid(),
//                detailBean.getRanges(),
//                detailBean.getRssi0(),
//                detailBean.getDtime(),
//                detailBean.getIn_time(),
//                detailBean.getMmac(),
//                detailBean.getRid(),
//                detailBean.getMac(),
//                config.getProid()
//        SingleOutputStreamOperator<Dws_n_st_basic> dws_n_st_basicBean = detailBean.keyBy(DetailBean::getMmac)
//                .intervalJoin(configBean.keyBy(Config::getMmac))
//                .between(Time.milliseconds(-5000), Time.milliseconds(5000))
//                .process(new ProcessJoinFunction<DetailBean, Config, Dws_n_st_basic>() {
//                    @Override
//                    public void processElement(DetailBean detailBean, Config config, ProcessJoinFunction<DetailBean, Config, Dws_n_st_basic>.Context context, Collector<Dws_n_st_basic> collector) throws Exception {
//                        collector.collect(new Dws_n_st_basic(detailBean.getDataid(),
//                                detailBean.getRanges(),
//                                detailBean.getRssi0(),
//                                detailBean.getDtime(),
//                                detailBean.getIn_time(),
//                                detailBean.getMmac(),
//                                detailBean.getRid(),
//                                detailBean.getMac(),
//                                config.getProid()));
//                    }
//                });
//
//        dws_n_st_basicBean.print();

        //分组聚合
//        SingleOutputStreamOperator<String> detailBeanTuple2KeyedStream = detailBean


        SingleOutputStreamOperator<UvCount> apply = Dws_n_st_basicBean.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Dws_n_st_basic>(Time.milliseconds(0)) {
                    @Override
                    public long extractTimestamp(Dws_n_st_basic dws_n_st_basic) {
                        return dws_n_st_basic.getDtime();
                    }
                })
                .keyBy(new KeySelector<Dws_n_st_basic, String>() {
                    @Override
                    public String getKey(Dws_n_st_basic dws_n_st_basic) throws Exception {
                        return dws_n_st_basic.getProid();
                    }
                }).window(TumblingEventTimeWindows.of(/*Time.milliseconds(2000),*/Time.milliseconds(1000)))
                .apply(new WindowFunction<Dws_n_st_basic, UvCount, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<Dws_n_st_basic> iterable, Collector<UvCount> collector) throws Exception {
                        HashSet<String> longHashSet = new HashSet<>();
                        Iterator<Dws_n_st_basic> iterator = iterable.iterator();
                        while (iterator.hasNext()) {
                            longHashSet.add(iterator.next().getMac());
                        }
                        Long size = Long.valueOf(longHashSet.size());
                        collector.collect(new UvCount(s, size,timeWindow.getStart(),timeWindow.getEnd()));
                    }
                });


        //.window(TumblingProcessingTimeWindows.of(/* Time.milliseconds(2000),*/Time.milliseconds(1000)))
//                .aggregate(new AggregateFunction<DetailBean, Acc, String>() {
//                    @Override
//                    public Acc createAccumulator() {
//                        Acc acc = new Acc();
//                        acc.init();
//                        return acc;
//                    }
//
//                    @Override
//                    public Acc add(DetailBean detailBean, Acc acc) {
//                        acc.count.getAndAdd(1);
//                        return acc;
//                    }
//
//                    @Override
//                    public String getResult(Acc acc) {
//                        return acc.toString();
//                    }
//
//                    @Override
//                    public Acc merge(Acc acc, Acc acc1) {
//                        acc.count.addAndGet(acc1.count.get());
//                        return acc;
//                    }
//                }, new WindowFunction<String, String, String, TimeWindow>() {
//                    @Override
//                    public void apply(String s, TimeWindow timeWindow, Iterable<String> iterable, Collector<String> collector) throws Exception {
//                        Iterator<String> iterator = iterable.iterator();
//                        while (iterator.hasNext()) {
//                            String next = iterator.next();
//                            collector.collect(s+" "+next+timeWindow.toString());
//                        }
//                    }
//                });

        apply.print();


        executionEnvironment.execute("My Kafka");

    }

    private static class Acc {
        AtomicLong count;

        public void init() {
            count = new AtomicLong(0);
        }

        public Acc() {
        }

        @Override
        public String toString() {
            return "" + count;
        }
    }
}
