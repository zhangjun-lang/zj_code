package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.MyClickHouseUtil;
import com.atguigu.util.MyKafkaUtil;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
//数据流：web/app -> Nginx -> 日志服务器(.log) -> Flume -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程序： Mock(lg.sh)     - >   Flume(f1) -> kafka(zk)  -> BaseLogApp -> kafka(zk) -> DwsTrafficPageViewWindow  -> ClickHouse(zk)
public class DwsTrafficPageViewWindow {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.读取kafka页面日志主题数据流
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //3.将每行数据转换为JSON对象并过滤
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                    collector.collect(jsonObject);
                }
            }
        });
        //4.提取事件事件生产WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                }));
        //5.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        //6.使用状态编程过滤出首页与商品详情页的独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> trafficHomeDetailDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            private ValueState<String> homeLastState;
            private ValueState<String> detailLastState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                ValueStateDescriptor<String> homeStateDes = new ValueStateDescriptor<>("home-state", String.class);
                ValueStateDescriptor<String> detailStateDes = new ValueStateDescriptor<>("detail-state", String.class);
                //设置TTL
                homeStateDes.enableTimeToLive(ttlConfig);
                detailStateDes.enableTimeToLive(ttlConfig);
                homeLastState = getRuntimeContext().getState(homeStateDes);
                detailLastState = getRuntimeContext().getState(detailStateDes);
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                //获取状态数据以及当前数据的日期
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);
                String homeLastDt = homeLastState.value();
                String detailLastDt = detailLastState.value();
                //定义访问当前首页或者详情页的数据
                long homeCt = 0L;
                long detailCt = 0L;

                //如果状态为空或者状态时间和当前时间不同，则为需要的数据
                if ("home".equals(jsonObject.getJSONObject("page").getString("page_id"))) {
                    if (homeLastDt == null || !homeLastDt.equals(curDt)) {
                        homeCt = 1L;
                        homeLastState.update(curDt);
                    }
                } else {
                    if (detailLastDt == null || !detailLastDt.equals(curDt)) {
                        detailCt = 1L;
                        detailLastState.update(curDt);
                    }
                }


                //满足任何一个数据不等于0，则写出
                if (homeCt == 1L || detailCt == 1L) {
                    collector.collect(new TrafficHomeDetailPageViewBean("", "",
                            homeCt,
                            detailCt,
                            ts));
                }
            }
        });
        //7.开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultDS = trafficHomeDetailDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))).reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
            @Override
            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean t1) throws Exception {
                trafficHomeDetailPageViewBean.setHomeUvCt(trafficHomeDetailPageViewBean.getHomeUvCt() + t1.getHomeUvCt());
                trafficHomeDetailPageViewBean.setGoodDetailUvCt(trafficHomeDetailPageViewBean.getGoodDetailUvCt() + t1.getGoodDetailUvCt());
                return trafficHomeDetailPageViewBean;
            }
        }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                //获取数据
                TrafficHomeDetailPageViewBean pageViewBean = iterable.iterator().next();
                //补充字段
                pageViewBean.setTs(System.currentTimeMillis());
                pageViewBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                pageViewBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                //输出数据
                collector.collect(pageViewBean);

            }
        });
        //8.将数据写出到Clickhouse
        resultDS.print(">>>>>>>>>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));
        //9.启动任务
        env.execute("DwsTrafficPageViewWindow");
    }
}
