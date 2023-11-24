package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.CartAddUuBean;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.MyClickHouseUtil;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
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
//数据流：Web/App -> nginx -> 业务服务器 -> Maxwell -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> clickhouse(DWS)
//程序： Mock -> Mysql -> Maxwell -> kafka(zk) -> DwdTradeCartAdd -> kafka(zk) -> DwsTradeCartAddUuWindow -> clickhouse(zk)
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.读取kafkaDWD层加购事实表
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //3.将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        //4.提取事件时间生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        String operate_time = jsonObject.getString("operate_time");
                        if (operate_time != null) {
                            return DateFormatUtil.toTs(operate_time, true);
                        } else {
                            return DateFormatUtil.toTs(jsonObject.getString("create_time"),true);
                        }
                    }
                }));
        //5.按照user_id分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(json -> json.getString("user_id"));
        //6.使用状态编程提取独立加购数据用户
        SingleOutputStreamOperator<CartAddUuBean> cartAddDS= keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {
            private ValueState<String> lastCartAddState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last_cart", String.class);
                stateDescriptor.enableTimeToLive(ttlConfig);
                lastCartAddState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<CartAddUuBean> collector) throws Exception {
                //获取状态数据以及当前数据的日期
                String lastDt = lastCartAddState.value();
                String operate_time = jsonObject.getString("operate_time");
                String curDt = null;
                if (operate_time != null) {
                    curDt = operate_time.split(" ")[0];
                } else {
                    String create_time = jsonObject.getString("create_time");
                    curDt = create_time.split(" ")[0];
                }
                if (lastDt == null || !lastDt.equals(curDt)) {
                    lastCartAddState.update(curDt);
                    collector.collect(new CartAddUuBean("", "", 1L, null));
                }
            }
        });
        //7.开窗聚合
        SingleOutputStreamOperator<CartAddUuBean> resultDS = cartAddDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean cartAddUuBean, CartAddUuBean t1) throws Exception {
                        cartAddUuBean.setCartAddUuCt((cartAddUuBean.getCartAddUuCt() + t1.getCartAddUuCt()));

                        return cartAddUuBean;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<CartAddUuBean> iterable, Collector<CartAddUuBean> collector) throws Exception {
                        CartAddUuBean next = iterable.iterator().next();
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setTs(System.currentTimeMillis());
                        collector.collect(next);
                    }
                });
        //8.将数据写出到ClickHouse
        resultDS.print(">>>>>>>>>>>>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));
        //9.启动任务
        env.execute("DwsTradeCartAddUuWindow");
    }
}
