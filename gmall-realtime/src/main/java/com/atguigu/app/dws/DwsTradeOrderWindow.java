package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TradeOrderBean;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.MyClickHouseUtil;
import com.atguigu.util.MyKafkaUtil;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
//数据流：Web/App -> nginx -> 业务服务器 -> Maxwell -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程序： Mock -> Mysql -> Maxwell -> kafka(zk) -> DwdTradeOrderPreProcess -> kafka(zk) -> DwdTradeOrderDetail -> kafka(zk) -> DwsTradeOrderWindow -> ClickHouse(ZK)
public class DwsTradeOrderWindow {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.读取kafkaDWD层下单主题数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_order_window";

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
        //4.按照 order_detail_id分组
        KeyedStream<JSONObject, String> keyedByDetailIdDS = jsonObjDS.keyBy(json -> json.getString("id"));
        //5.针对 order_detail_id 进行去重
        SingleOutputStreamOperator<JSONObject> filterDS = keyedByDetailIdDS.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.seconds(5))
                        .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                        .build();
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("is_exists", String.class);
                stateDescriptor.enableTimeToLive(ttlConfig);
                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String state = valueState.value();
                if (state == null) {
                    valueState.update("1");
                    return true;
                } else {
                    return false;
                }

            }
        });
        //6. 提取时间时间生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = filterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {

                        return DateFormatUtil.toTs(jsonObject.getString("create_time"), true);
                    }
                }));
        //7.按照 user_id 分组
        KeyedStream<JSONObject, String> keyedByUidDS = jsonObjWithWmDS.keyBy(json -> json.getString("user_id"));
        //8.提取独立下单用户
        SingleOutputStreamOperator<TradeOrderBean> tradeOrderDS = keyedByUidDS.map(new RichMapFunction<JSONObject, TradeOrderBean>() {
            private ValueState<String> lastOrderDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastOrderDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_order", String.class));
            }

            @Override
            public TradeOrderBean map(JSONObject jsonObject) throws Exception {
                String lastOrderDt = lastOrderDtState.value();
                String curDt = jsonObject.getString("create_time").split(" ")[0];
                //定义当天下单人数以及新增下单人数
                Long orderUniqueUserCount = 0L;
                Long orderNewUserCount = 0L;
                //判断状态是否为null
                if (lastOrderDt == null) {
                    orderUniqueUserCount = 1L;
                    orderNewUserCount = 1L;
                    lastOrderDtState.update(curDt);
                } else if (!lastOrderDt.equals(curDt)) {
                    orderUniqueUserCount = 1L;
                    lastOrderDtState.update(curDt);
                }
                //取出下单件数以及单价
                Integer sku_num = jsonObject.getInteger("sku_num");
                Double order_price = jsonObject.getDouble("order_price");
                Double split_activity_amount = jsonObject.getDouble("split_activity_amount");
                if (split_activity_amount == null) {
                    split_activity_amount = 0.0D;
                }
                Double split_coupon_amount = jsonObject.getDouble("split_coupon_amount");
                if (split_coupon_amount == null) {
                    split_coupon_amount = 0.0D;
                }
                return new TradeOrderBean("", "",
                        orderUniqueUserCount,
                        orderNewUserCount,
                        split_activity_amount,
                        split_coupon_amount,
                        sku_num * order_price,
                        null);
            }
        });
        //9.开窗聚合
        SingleOutputStreamOperator<TradeOrderBean> resultDS = tradeOrderDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean t1) throws Exception {
                        value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + t1.getOrderUniqueUserCount());
                        value1.setOrderNewUserCount(value1.getOrderNewUserCount() + t1.getOrderNewUserCount());
                        value1.setOrderOriginalTotalAmount(value1.getOrderOriginalTotalAmount() + t1.getOrderOriginalTotalAmount());
                        value1.setOrderActivityReduceAmount(value1.getOrderActivityReduceAmount() + t1.getOrderActivityReduceAmount());
                        value1.setOrderCouponReduceAmount(value1.getOrderCouponReduceAmount() + t1.getOrderCouponReduceAmount());
                        return value1;
                    }
                }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradeOrderBean> iterable, Collector<TradeOrderBean> collector) throws Exception {
                        TradeOrderBean next = iterable.iterator().next();
                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        collector.collect(next);
                    }
                });
        //10.将数据写出到ClickHouse
        resultDS.print(">>>>>>>>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_order_window values(?,?,?,?,?,?,?,?)"));
        //11.执行任务
        env.execute("DwsTradeOrderWindow");
    }
}
