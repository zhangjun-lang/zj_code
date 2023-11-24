package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.util.DateFormatUtil;
import com.atguigu.util.MyKafkaUtil;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
//数据流：web/app -> Nginx -> 日志服务器(.log) -> Flume -> kafka(ODS) -> FlinkApp -> kafka(DWD) -> FlinkApp -> kafka(DWD)
//程序： Mock(lg.sh)     - >   Flume(f1) -> kafka(zk)  -> BaseLogApp -> kafka(zk) -> DwdTrafficUniqueVisitorDetail -> kafka(zk)
public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取kafka页面日志主题
        String topic = "dwd_traffic_page_log";
        String groupId = "Unique_Visitor_Detail ";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //3.过滤上一跳页面不为null的数据并将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageId == null) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(s);
                }

            }
        });
        //4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //5.使用状态编程实现Mid的去重
        SingleOutputStreamOperator<JSONObject> uvDs = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);
                //设置状态的TTL
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stringValueStateDescriptor.enableTimeToLive(ttlConfig);
                lastVisitState = getRuntimeContext().getState(stringValueStateDescriptor);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                //获取状态数据&转换为当前数据中的时间戳为日期
                String lastDate = lastVisitState.value();
                Long ts = jsonObject.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);
                if (lastDate == null || !lastDate.equals(curDate)) {
                    lastVisitState.update(curDate);
                    return true;
                } else {
                    return false;
                }
            }
        });
        //6.将数据写到kafka
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        uvDs.print(">>>>>>>>>>>>>>");
        uvDs.map(JSONAware::toJSONString).
                addSink(MyKafkaUtil.getFlinkKafkaProducer((targetTopic)));
        //7.启动任务
        env.execute("DwdTrafficUniqueVisitorDetail");
    }

}
