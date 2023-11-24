package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.TradeUserSpuOrderBean;
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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.rmi.activation.ActivationSystem;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
//数据流：Web/App -> nginx -> 业务服务器 -> Maxwell -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程序： Mock -> Mysql -> Maxwell -> kafka(zk) -> DwdTradeOrderPreProcess -> kafka(zk) -> DwdTradeOrderDetail -> kafka(zk) -> DwsTradeUserSpuOrderWindow -> ClickCHouse(ZK)
public class DwsTradeUserSpuOrderWindow {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.读取kafkaDWD层下单主题数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_trademark_category_user_order_window";

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
        //6.将数据转换为JavaBean对象
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuDS = filterDS.map(jsonObject -> {
            HashSet<String> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getString("order_id"));
            return TradeUserSpuOrderBean.builder()
                    .skuId(jsonObject.getString("sku_id"))
                    .userId(jsonObject.getString("user_id"))
                    .orderAmount(jsonObject.getDouble("split_total_amount"))
                    .orderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(jsonObject.getString("create_time"), true))
                    .build();
        });
        tradeUserSpuDS.print("tradeUserSpuDS>>>>>>>>>>>>>>>>>");
        //7.关联Sku_info维表，补充 spu_id,tm_id, category3_id
//        tradeUserSpuDS.map(new RichMapFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean>() {
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                //创建Phoenix连接池
//
//            }
//
//            @Override
//            public TradeUserSpuOrderBean map(TradeUserSpuOrderBean tradeUserSpuOrderBean) throws Exception {
//                //查询维表，将查询的信息补充到JavaBean
//                return null;
//            }
//        });
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuWithSkuDS = AsyncDataStream.unorderedWait(tradeUserSpuDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        //System.out.println(input.getSkuId());
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setSpuId(dimInfo.getString("SPU_ID"));
                        tradeUserSpuOrderBean.setTrademarkId(dimInfo.getString("TM_ID"));
                        tradeUserSpuOrderBean.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));

                    }
                },
                100,
                TimeUnit.SECONDS);
        tradeUserSpuWithSkuDS.print("tradeUserSpuWithSkuDS>>>>>>>>>>>>>>>");
        //8.提取事件时间生成WaterMark
        SingleOutputStreamOperator<TradeUserSpuOrderBean> tradeUserSpuWithWmDS= tradeUserSpuWithSkuDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeUserSpuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeUserSpuOrderBean>() {
                    @Override
                    public long extractTimestamp(TradeUserSpuOrderBean tradeUserSpuOrderBean, long l) {
                        return tradeUserSpuOrderBean.getTs();
                    }
                }));
        //9.分组聚合开窗
        KeyedStream<TradeUserSpuOrderBean, Tuple4<String, String, String, String>> keyedStream = tradeUserSpuWithWmDS.keyBy(new KeySelector<TradeUserSpuOrderBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TradeUserSpuOrderBean tradeUserSpuOrderBean) throws Exception {
                return new Tuple4<>(tradeUserSpuOrderBean.getUserId(),
                        tradeUserSpuOrderBean.getSpuId(),
                        tradeUserSpuOrderBean.getTrademarkId(),
                        tradeUserSpuOrderBean.getCategory3Id());
            }
        });
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceDS = keyedStream.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradeUserSpuOrderBean>() {
                    @Override
                    public TradeUserSpuOrderBean reduce(TradeUserSpuOrderBean value1, TradeUserSpuOrderBean t1) throws Exception {
                        value1.getOrderIdSet().addAll(t1.getOrderIdSet());
                        value1.setOrderAmount(value1.getOrderAmount() + t1.getOrderAmount());
                        return value1;
                    }
                }, new WindowFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> key, TimeWindow window, Iterable<TradeUserSpuOrderBean> iterable, Collector<TradeUserSpuOrderBean> collector) throws Exception {
                        TradeUserSpuOrderBean next = iterable.iterator().next();
                        next.setTs(System.currentTimeMillis());
                        next.setOrderCount((long) next.getOrderIdSet().size());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        collector.collect(next);

                    }
                });
        //10.关联spu,tm,category维表补充相应信息
        //关联Spu表
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithSpuDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getSpuId();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setSpuName(dimInfo.getString("SPU_NAME"));

                    }
                }, 100, TimeUnit.SECONDS);
        //关联Tm表
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithTmDS = AsyncDataStream.unorderedWait(reduceWithSpuDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setTrademarkName(dimInfo.getString("TM_NAME"));
                    }
                }, 100, TimeUnit.SECONDS);
        //关联Category3
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithCategory3DS = AsyncDataStream.unorderedWait(reduceWithTmDS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setCategory3Name(dimInfo.getString("NAME"));
                        tradeUserSpuOrderBean.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                    }
                }, 100, TimeUnit.SECONDS);
        //关联Category2
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithCategory2DS = AsyncDataStream.unorderedWait(reduceWithCategory3DS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setCategory2Name(dimInfo.getString("NAME"));
                        tradeUserSpuOrderBean.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                    }
                }, 100, TimeUnit.SECONDS);
        //关联Category1
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reduceWithCategory1DS = AsyncDataStream.unorderedWait(reduceWithCategory2DS,
                new DimAsyncFunction<TradeUserSpuOrderBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeUserSpuOrderBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeUserSpuOrderBean tradeUserSpuOrderBean, JSONObject dimInfo) {
                        tradeUserSpuOrderBean.setCategory1Name(dimInfo.getString("NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        //11.将数据写入出CLickHouse
        reduceWithCategory1DS.print(">>>>>>>>>>>>>>>>>>");
        reduceWithCategory1DS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));
        //12.启动任务
        env.execute("DwsTradeUserSpuOrderWindow");

    }
}
