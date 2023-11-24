package jun.zhang.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import jun.zhang.app.func.DimAsyncFunction;
import jun.zhang.bean.TradeProvinceOrderWindow;
import jun.zhang.util.DateFormatUtil;
import jun.zhang.util.MyClickHouseUtil;
import jun.zhang.util.MyKafkaUtil;
import jun.zhang.util.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
//数据流：Web/App -> nginx -> 业务服务器 -> Maxwell -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程序： Mock -> Mysql -> Maxwell -> kafka(zk) -> DwdTradeOrderPreProcess -> kafka(zk) -> DwdTradeOrderDetail -> kafka(zk) -> DwsTradeProvinceOrderWindow -> ClickCHouse(ZK)
public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.读取kafkaDWD层下单主题数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_province_order_window";


        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //3.将每行数据转换成JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("Value>>>>>>>>>>>>>>>>>>>" + s);
                }

            }
        });
        //4.按照 order_detail_id分组, 去重(取最后一条的数据)

        KeyedStream<JSONObject, String> keyedByDetailIdDS = jsonObjDS.keyBy(json -> json.getString("id"));
        SingleOutputStreamOperator<JSONObject> filterDS = keyedByDetailIdDS.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("value-state", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject lastValue = valueState.value();
                if (lastValue == null) {
                    valueState.update(jsonObject);
                    long processingTime = context.timerService().currentProcessingTime();
                    context.timerService().registerProcessingTimeTimer(processingTime + 5000L);
                } else {
                    //取出状态数据以及当前数据中的时间
                    String lastTs = lastValue.getString("row_op_ts");
                    String curTs = jsonObject.getString("row_op_ts");
                    if (TimestampLtz3CompareUtil.compare(lastTs, curTs) != 1) {
                        valueState.update(jsonObject);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                //输出数据并清空状态
                out.collect(valueState.value());
                valueState.clear();
            }
        });
        //5.将每行数据转换维JAVABean
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceOrderDS = filterDS.map(line -> {
            HashSet<String> orderIdSet = new HashSet<>();
            orderIdSet.add(line.getString("order_id"));
            return new TradeProvinceOrderWindow("", "",
                    line.getString("province_id"),
                    "",
                    0L,
                    orderIdSet,
                    line.getDouble("split_total_amount"),
                    DateFormatUtil.toTs(line.getString("create_time"), true));
        });
        //6.提取时间戳生成WaterMark
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceOrderWithWmDS = tradeProvinceOrderDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
                    @Override
                    public long extractTimestamp(TradeProvinceOrderWindow tradeProvinceOrderWindow, long l) {
                        return tradeProvinceOrderWindow.getTs();
                    }
                }));
        //7.分组开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderWindow> reduceDS = tradeProvinceOrderWithWmDS.keyBy(TradeProvinceOrderWindow::getProvinceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeProvinceOrderWindow>() {
                    @Override
                    public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow t1) throws Exception {
                        value1.getOrderIdSet().addAll(t1.getOrderIdSet());
                        value1.setOrderAmount(value1.getOrderAmount() + t1.getOrderAmount());
                        return value1;
                    }
                }, new WindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderWindow> iterable, Collector<TradeProvinceOrderWindow> collector) throws Exception {
                        TradeProvinceOrderWindow provinceOrderWindow = iterable.iterator().next();
                        provinceOrderWindow.setTs(System.currentTimeMillis());

                        provinceOrderWindow.setOrderCount((long) provinceOrderWindow.getOrderIdSet().size());
                        provinceOrderWindow.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        provinceOrderWindow.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        collector.collect(provinceOrderWindow);
                    }
                });
        reduceDS.print("reduceDS>>>>>>>>>>>>>>>>");
        //8.关联维表补充省份名称字段
        SingleOutputStreamOperator<TradeProvinceOrderWindow> reduceWithProvinceDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<TradeProvinceOrderWindow>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(TradeProvinceOrderWindow input) {
                        return input.getProvinceId();
                    }

                    @Override
                    public void join(TradeProvinceOrderWindow tradeProvinceOrderWindow, JSONObject dimInfo) {
                        tradeProvinceOrderWindow.setProvinceName(dimInfo.getString("NAME"));
                    }
                }, 100, TimeUnit.SECONDS);
        //9.将数据写出到ClickHouse
        reduceWithProvinceDS.print("reduceWithProvinceDS>>>>>>>>>>>>>>>>>");
        reduceWithProvinceDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"));

        //10.启动任务
        env.execute("DwsTradeProvinceOrderWindow");
    }
}
