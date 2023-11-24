package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.TradeTrademarkCategoryUserRefundBean;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.MyClickHouseUtil;
import com.atguigu.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
//数据流：Web/App -> nginx -> 业务服务器 -> Maxwell -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> ClickHouse
//程序： Mock -> Mysql -> Maxwell -> kafka(zk) -> DwdTradeOrderRefund -> kafka(zk) -> DwsTradeTrademarkCategoryUserRefundWindow - > CLickCHouse(ZK)
public class DwsTradeTrademarkCategoryUserRefundWindow {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.读取kafka DWD层退单主题数据
        String topic = "dwd_trade_order_refund";
        String groupId = "dws_trade_trademark_category_user_refund_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //3.将每行数据转换为JavaBean
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tradeTrademarkCategoryDS = kafkaDS.map(line -> {
            HashSet<String> orderIds = new HashSet<>();

            JSONObject jsonObject = JSON.parseObject(line);
            orderIds.add(jsonObject.getString("order_id"));
            return TradeTrademarkCategoryUserRefundBean.builder()
                    .skuId(jsonObject.getString("sku_id"))
                    .userId(jsonObject.getString("user_id"))
                    .orderIdSet(orderIds)
                    .ts(DateFormatUtil.toTs(jsonObject.getString("create_time"), true))
                    .build();
        });
        //4.关联维表信息补充tm_id以及category3_id
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tradeWithSkuDS = AsyncDataStream.unorderedWait(tradeTrademarkCategoryDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, JSONObject dimInfo) {
                        tradeTrademarkCategoryUserRefundBean.setTrademarkId(dimInfo.getString("TM_ID"));
                        tradeTrademarkCategoryUserRefundBean.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                    }
                }, 100, TimeUnit.SECONDS);
        //5.分组开窗聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceDS = tradeWithSkuDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public long extractTimestamp(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, long l) {
                        return tradeTrademarkCategoryUserRefundBean.getTs();
                    }
                })).keyBy(new KeySelector<TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) throws Exception {
                return new Tuple3<>(tradeTrademarkCategoryUserRefundBean.getUserId(), tradeTrademarkCategoryUserRefundBean.getTrademarkId(), tradeTrademarkCategoryUserRefundBean.getCategory3Id());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean t1) throws Exception {
                        value1.getOrderIdSet().addAll(t1.getOrderIdSet());
                        return value1;
                    }
                }, new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple3<String, String, String> stringStringStringTuple3, TimeWindow window, Iterable<TradeTrademarkCategoryUserRefundBean> iterable, Collector<TradeTrademarkCategoryUserRefundBean> collector) throws Exception {
                        TradeTrademarkCategoryUserRefundBean next = iterable.iterator().next();
                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setRefundCount((long) next.getOrderIdSet().size());
                        collector.collect(next);
                    }
                });
        //6，关联维表补充其他字段
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceWithTmDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setTrademarkName(dimInfo.getString("TM_NAME"));

                    }
                }, 100,
                TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceWith3DS = AsyncDataStream.unorderedWait(reduceWithTmDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory3Name(dimInfo.getString("NAME"));
                        input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));

                    }
                }, 100,
                TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceWith2DS = AsyncDataStream.unorderedWait(reduceWith3DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory2Name(dimInfo.getString("NAME"));
                        input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));

                    }
                }, 100,
                TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceWith1DS = AsyncDataStream.unorderedWait(reduceWith2DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory1Name(dimInfo.getString("NAME"));

                    }
                }, 100,
                TimeUnit.SECONDS);
        //7.将数据写出到Clickhouse
        reduceWith1DS.print(">>>>>>>>>>>>>>>>>>>");
        reduceWith1DS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //8.启动任务
        env.execute("DwsTradeTrademarkCategoryUserRefundWindow");
    }
}
